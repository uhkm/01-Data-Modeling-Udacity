CREATE TABLE fact_time_spent (
	trip_id VARCHAR(50),
    rider_id INT,
    start_station_id VARCHAR(50),
    end_station_id VARCHAR(50),
	rideable_type VARCHAR(75),
    duration_minutes INT,
	start_at_timestamp DATETIME2(0),
    ride_date DATE
);
-- Load data into fact_time_spent
INSERT INTO fact_time_spent (trip_id, rider_id, start_station_id, end_station_id, rideable_type, duration_minutes, start_at_timestamp, ride_date)
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