CREATE TABLE station_dimension (
    station_id VARCHAR(50),
    station_name VARCHAR(75),
    latitude FLOAT,
    longitude FLOAT
);

-- Load data into station_dimension
INSERT INTO station_dimension (station_id, station_name, latitude, longitude)
SELECT DISTINCT
    station_id,
    name AS station_name,
    latitude,
    longitude
FROM dbo.stage_station;