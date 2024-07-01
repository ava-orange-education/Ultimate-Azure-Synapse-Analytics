--Step 1
SELECT *
FROM sys.dm_pdw_exec_requests
WHERE [status] NOT IN ('Completed','Failed','Cancelled')
AND session_id <> session_id()
ORDER BY submit_time DESC;

SELECT TOP 10 *
FROM sys.dm_pdw_exec_requests
ORDER BY total_elapsed_time DESC;
--------------------------------------------------------------------------------------------------------------------------------
--******************************************************************************************************************************
----------------------------------------------------------------------------------------------------------------------------------
--Step 2
DECLARE @QID VARCHAR(16) = '<request_id>', @ShowActiveOnly BIT = 1; 

-- Retrieve session_id of QID
DECLARE @session_id VARCHAR(16) = (SELECT session_id FROM sys.dm_pdw_exec_requests WHERE request_id = @QID);

-- Blocked by Compilation or Resource Allocation (Concurrency)
SELECT @session_id AS session_id, @QID AS request_id, -1 AS [StepIndex], 'Compilation' AS [Phase],
   'Blocked waiting on '
       + MAX(CASE WHEN waiting.type = 'CompilationConcurrencyResourceType' THEN 'Compilation Concurrency'
                  WHEN waiting.type LIKE 'Shared-%' THEN ''
                  ELSE 'Resource Allocation (Concurrency)' END)
       + MAX(CASE WHEN waiting.type LIKE 'Shared-%' THEN ' for ' + REPLACE(waiting.type, 'Shared-', '')
             ELSE '' END) AS [Description],
   MAX(waiting.request_time) AS [StartTime], GETDATE() AS [EndTime],
   DATEDIFF(ms, MAX(waiting.request_time), GETDATE())/1000.0 AS [Duration],
   NULL AS [Status], NULL AS [EstimatedRowCount], NULL AS [ActualRowCount], NULL AS [TSQL]
FROM sys.dm_pdw_waits waiting
WHERE waiting.session_id = @session_id
   AND ([type] LIKE 'Shared-%' OR
      [type] in ('ConcurrencyResourceType', 'UserConcurrencyResourceType', 'CompilationConcurrencyResourceType'))
   AND [state] = 'Queued'
GROUP BY session_id 

-- Blocked by another query
UNION ALL
SELECT @session_id AS session_id, @QID AS request_id, -1 AS [StepIndex], 'Compilation' AS [Phase],
   'Blocked by ' + blocking.session_id + ':' + blocking.request_id + ' when requesting ' + waiting.type + ' on '
   + QUOTENAME(waiting.object_type) + waiting.object_name AS [Description],
   waiting.request_time AS [StartTime], GETDATE() AS [EndTime],
   DATEDIFF(ms, waiting.request_time, GETDATE())/1000.0 AS [Duration],
   NULL AS [Status], NULL AS [EstimatedRowCount], NULL AS [ActualRowCount],
   COALESCE(blocking_exec_request.command, blocking_exec_request.command2) AS [TSQL]
FROM sys.dm_pdw_waits waiting
   INNER JOIN sys.dm_pdw_waits blocking
      ON waiting.object_type = blocking.object_type
      AND waiting.object_name = blocking.object_name
   INNER JOIN sys.dm_pdw_exec_requests blocking_exec_request
      ON blocking.request_id = blocking_exec_request.request_id
WHERE waiting.session_id = @session_id AND waiting.state = 'Queued'
   AND blocking.state = 'Granted' AND waiting.type != 'Shared' 

-- Request Steps
UNION ALL
SELECT @session_id AS session_id, @QID AS request_id, step_index AS [StepIndex],
       'Execution' AS [Phase], operation_type + ' (' + location_type + ')' AS [Description],
       start_time AS [StartTime], end_time AS [EndTime],
       total_elapsed_time/1000.0 AS [Duration], [status] AS [Status],
       CASE WHEN estimated_rows > -1 THEN estimated_rows END AS [EstimatedRowCount],
       CASE WHEN row_count > -1 THEN row_count END AS [ActualRowCount],
       command AS [TSQL]
FROM sys.dm_pdw_request_steps
WHERE request_id = @QID
   AND [status] = CASE @ShowActiveOnly WHEN 1 THEN 'Running' ELSE [status] END
ORDER BY StepIndex;
--------------------------------------------------------------------------------------------------------------------------------
--******************************************************************************************************************************
----------------------------------------------------------------------------------------------------------------------------------
--Step 3
DECLARE @QID VARCHAR(16) = '<request_id>', @StepIndex INT = <StepIndex>, @ShowActiveOnly BIT = 1;
WITH dists
AS (SELECT request_id, step_index, 'sys.dm_pdw_sql_requests' AS source_dmv,
       distribution_id, pdw_node_id, spid, 'NativeSQL' AS [type], [status],
       start_time, end_time, total_elapsed_time, row_count
    FROM sys.dm_pdw_sql_requests
    WHERE request_id = @QID AND step_index = @StepIndex
    UNION ALL
    SELECT request_id, step_index, 'sys.dm_pdw_dms_workers' AS source_dmv,
       distribution_id, pdw_node_id, sql_spid AS spid, [type],
       [status], start_time, end_time, total_elapsed_time, rows_processed as row_count
    FROM sys.dm_pdw_dms_workers
    WHERE request_id = @QID AND step_index = @StepIndex
   )
SELECT sr.step_index, sr.distribution_id, sr.pdw_node_id, sr.spid,
       sr.type, sr.status, sr.start_time, sr.end_time,
       sr.total_elapsed_time, sr.row_count, owt.wait_type, owt.wait_time
FROM dists sr
   LEFT JOIN sys.dm_pdw_nodes_exec_requests owt
      ON sr.pdw_node_id = owt.pdw_node_id
         AND sr.spid = owt.session_id
         AND ((sr.source_dmv = 'sys.dm_pdw_sql_requests'
                 AND sr.status = 'Running') -- sys.dm_pdw_sql_requests status
              OR (sr.source_dmv = 'sys.dm_pdw_dms_requests'
                     AND sr.status not LIKE 'Step[CE]%')) -- sys.dm_pdw_dms_workers final statuses
WHERE sr.request_id = @QID
      AND ((sr.source_dmv = 'sys.dm_pdw_sql_requests' AND sr.status =
               CASE WHEN @ShowActiveOnly = 1 THEN 'Running' ELSE sr.status END)
           OR (sr.source_dmv = 'sys.dm_pdw_dms_workers' AND sr.status NOT LIKE
                  CASE WHEN @ShowActiveOnly = 1 THEN 'Step[CE]%' ELSE '' END))
      AND sr.step_index = @StepIndex
ORDER BY distribution_id