-- Use CETAS to export select statement
IF OBJECT_ID('dbo.station_dimension') IS NOT NULL
BEGIN
  DROP EXTERNAL TABLE [dbo].[station_dimension];
END

CREATE EXTERNAL TABLE dbo.station_dimension

WITH (
    LOCATION    = 'station_dimension',
    DATA_SOURCE = [adls2filesystemuhk_adls2uhk_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT DISTINCT
    station_id,
    name AS station_name,
    latitude,
    longitude
FROM dbo.stage_station;
GO

-- Query the newly created CETAS external table
SELECT TOP 100 * FROM dbo.station_dimension
GO