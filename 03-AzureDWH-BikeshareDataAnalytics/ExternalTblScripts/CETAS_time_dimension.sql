-- Use CETAS to export select statement
IF OBJECT_ID('dbo.time_dimension') IS NOT NULL
BEGIN
  DROP EXTERNAL TABLE [dbo].[time_dimension];
END

CREATE EXTERNAL TABLE dbo.time_dimension

WITH (
    LOCATION    = 'time_dimension',
    DATA_SOURCE = [adls2filesystemuhk_adls2uhk_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT DISTINCT
    CAST(start_at AS DATE) AS ride_date,
	YEAR(start_at) AS year_of_date,
	DATEPART(QUARTER, start_at) AS quarter_of_year,
	DATENAME(MONTH, start_at) AS month_of_date,
    DATENAME(DW, start_at) AS day_of_week
FROM dbo.stage_trip;
GO

-- Query the newly created CETAS external table
SELECT TOP 100 * FROM dbo.time_dimension
GO