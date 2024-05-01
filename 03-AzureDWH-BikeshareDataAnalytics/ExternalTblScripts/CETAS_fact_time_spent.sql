-- Use CETAS to export select statement
IF OBJECT_ID('dbo.fact_trip') IS NOT NULL
BEGIN
  DROP EXTERNAL TABLE [dbo].[fact_trip];
END

CREATE EXTERNAL TABLE dbo.fact_trip

WITH (
    LOCATION    = 'fact_trip',
    DATA_SOURCE = [adls2filesystemuhk_adls2uhk_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT
    t.trip_id,
	t.rider_id,
    t.start_station_id,
    t.end_station_id,
    t.rideable_type,
    DATEDIFF(minute, t.start_at, t.ended_at) AS duration_minutes,
    t.start_at AS start_at_timestamp,
    CAST(t.start_at as DATE) AS ride_date
FROM dbo.stage_trip t
JOIN dbo.time_dimension td ON CAST(t.start_at as DATE) = td.ride_date
JOIN dbo.station_dimension ss ON t.start_station_id = ss.station_id
JOIN dbo.station_dimension es ON t.end_station_id = es.station_id
JOIN dbo.rider_dimension rd ON t.rider_id = rd.rider_id;
GO

-- Query the newly created CETAS external table
SELECT TOP 100 * FROM dbo.fact_trip
GO