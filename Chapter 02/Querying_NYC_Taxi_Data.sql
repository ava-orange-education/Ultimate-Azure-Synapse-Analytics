SELECT TOP 100 *
FROM OPENROWSET(
        BULK 'https://[linked-storage-account].dfs.core.windows.net/[container-name]/NYCTripSmall.parquet',
        FORMAT='PARQUET'
    ) AS [nyc_taxi]
