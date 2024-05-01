-- Load data into time_dimension
INSERT INTO time_dimension (ride_date, year_of_date, quarter_of_year, month_of_date, day_of_week)
SELECT DISTINCT
    DATE(start_at) AS ride_date,
	EXTRACT(YEAR FROM start_at) AS year_of_date,
	EXTRACT(QUARTER FROM start_at) AS quarter_of_year,
	EXTRACT(MONTH FROM start_at) AS month_of_date,
    EXTRACT(DOW FROM start_at) AS day_of_week
FROM trip;

-- Load data into station_dimension
INSERT INTO station_dimension (station_id, station_name, latitude, longitude)
SELECT DISTINCT
    station_id,
    name AS station_name,
    latitude,
    longitude
FROM station;

-- Load data into rider_dimension
INSERT INTO rider_dimension (rider_id, account_start_date, account_end_date, is_member, age_at_ride_time)
SELECT DISTINCT
    rider_id,
    account_start_date,
    account_end_date,
    is_member,
    EXTRACT(YEAR FROM AGE(DATE(account_start_date), birthday)) AS age_at_ride_time
FROM rider;

-- Load data into payment_dimension
INSERT INTO fact_amount_Spent (payment_id, rider_id, payment_date, amount)
SELECT DISTINCT
	p.payment_id,
	r.rider_id,
    p.date,
    p.amount
FROM payment p 
JOIN rider_dimension r
on p.rider_id = r.rider_id;

-- Load data into ride_fact
INSERT INTO fact_time_spent (trip_id, rider_id, start_station_id, end_station_id, rideable_type, duration_minutes, start_at_timestamp, ride_date)
SELECT
    t.trip_id,
	t.rider_id,
    t.start_station_id,
    t.end_station_id,
    t.rideable_type,
    EXTRACT(EPOCH FROM (t.ended_at - t.start_at)) / 60 AS duration_minutes,
--     p.amount AS amount_spent,
    t.start_at AS start_at_timestamp,
    DATE(t.start_at) AS ride_date
FROM trip t
-- JOIN fact_payment p ON t.rider_id = p.rider_id AND DATE(t.start_at) = p.payment_date
JOIN time_dimension td ON DATE(t.start_at) = td.ride_date
JOIN station_dimension ss ON t.start_station_id = ss.station_id
JOIN station_dimension es ON t.end_station_id = es.station_id
JOIN rider_dimension rd ON t.rider_id = rd.rider_id;








