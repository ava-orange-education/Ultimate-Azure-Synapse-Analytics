-- Check for data skew in a distributed table
DBCC PDW_SHOWSPACEUSED('dbo.FactInternetSales');

-- Find tables with more than 10% data skew
SELECT * FROM dbo.vTableSizes
WHERE two_part_name IN (
    SELECT two_part_name FROM dbo.vTableSizes
    WHERE row_count > 0
    GROUP BY two_part_name
    HAVING (MAX(row_count * 1.000) - MIN(row_count * 1.000)) / MAX(row_count * 1.000) >= 0.10
)
ORDER BY two_part_name, row_count;
