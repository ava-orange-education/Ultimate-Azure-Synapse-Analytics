CREATE TABLE dbo.NYCTaxidata (
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
	DISTRIBUTION = ROUND_ROBIN
	)
GO

SELECT * FROM dbo.NYCTaxidata