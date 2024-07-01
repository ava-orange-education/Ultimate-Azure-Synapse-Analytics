IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'ultimateazure') 
	CREATE EXTERNAL DATA SOURCE [ultimateazure] 
	WITH (
		LOCATION = 'abfss://ultimatefile@ultimateazure.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE dbo.Taxidata (
	[VendorID] int,
	[lpep_pickup_datetime] datetime2(7),
	[lpep_dropoff_datetime] datetime2(7),
	[store_and_fwd_flag] nvarchar(4000),
	[RatecodeID] bigint,
	[PULocationID] int,
	[DOLocationID] int,
	[passenger_count] bigint,
	[trip_distance] float,
	[fare_amount] float,
	[extra] float,
	[mta_tax] float,
	[tip_amount] float,
	[tolls_amount] float,
	[ehail_fee] float,
	[improvement_surcharge] float,
	[total_amount] float,
	[payment_type] bigint,
	[trip_type] bigint,
	[congestion_surcharge] float
	)
	WITH (
	LOCATION = 'NYCTripSmall.parquet',
	DATA_SOURCE = [ultimateazure],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO




SELECT TOP 100 * FROM dbo.Taxidata
GO