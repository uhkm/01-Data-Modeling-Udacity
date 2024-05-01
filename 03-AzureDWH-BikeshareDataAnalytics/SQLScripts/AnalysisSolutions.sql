--3b. Based on how many minutes the rider spends on a bike per month
SELECT
    rd.rider_id,
    EXTRACT(MONTH FROM t.ride_date) AS ride_month,
    SUM(duration_minutes) AS total_duration,
    ROUND(AVG(amount::numeric),2) AS average_amount
FROM fact_time_spent t
JOIN rider_dimension rd ON t.rider_id = rd.rider_id
JOIN fact_amount_spent p ON t.ri der_id = p.rider_id
GROUP BY rd.rider_id, ride_month
ORDER BY rd.rider_id, ride_month;

--3a. Based on how many rides the rider averages per month
SELECT
    rd.rider_id,
    COUNT(*) AS total_rides,
    ROUND(AVG(amount::numeric),2) AS average_amount
FROM fact_amount_spent p
JOIN rider_dimension rd ON p.rider_id = rd.rider_id
JOIN fact_time_spent t ON rd.rider_id = t.rider_id
GROUP BY rd.rider_id
ORDER BY total_rides DESC;


-- 2b. Per member, based on the age of the rider at account start (Check for the age again)
SELECT
    rd.age_at_ride_time,
    rd.is_member,
    SUM(amount) AS total_amount
FROM fact_amount_spent p
JOIN rider_dimension rd ON p.rider_id = rd.rider_id
GROUP BY rd.age_at_ride_time, rd.is_member
ORDER BY rd.age_at_ride_time, rd.is_member;


-- 2a. Per month, quarter, year
SELECT
    EXTRACT(YEAR FROM payment_date) AS payment_year,
    EXTRACT(QUARTER FROM payment_date) AS payment_quarter,
    EXTRACT(MONTH FROM payment_date) AS payment_month,
    SUM(amount) AS total_amount
FROM fact_amount_spent
GROUP BY payment_year, payment_quarter, payment_month
ORDER BY payment_year, payment_quarter, payment_month;


--d. Based on whether the rider is a member or a casual rider

SELECT
    rd.is_member,
    ROUND(AVG(duration_minutes),2) AS average_duration
FROM fact_time_spent t
JOIN rider_dimension rd ON t.rider_id = rd.rider_id
GROUP BY rd.is_member;


-- c. Based on the age of the rider at the time of the ride (Check for age validity)
SELECT
    rd.age_at_ride_time,
    AVG(duration_minutes) AS average_duration
FROM fact_time_spent t
JOIN rider_dimension rd ON t.rider_id = rd.rider_id
GROUP BY rd.age_at_ride_time
ORDER BY rd.age_at_ride_time;


--b. Based on which station is the starting and/or ending station
SELECT
    ss.station_name AS start_station,
    es.station_name AS end_station,
    ROUND(AVG(duration_minutes),2) AS average_duration
FROM fact_time_spent t
JOIN station_dimension ss ON t.start_station_id = ss.station_id
JOIN station_dimension es ON t.end_station_id = es.station_id
GROUP BY start_station, end_station
having ss.station_name <> es.station_name
ORDER BY start_station, end_station;

-- a. Based on date and time factors such as day of the week and time of day
SELECT
    CASE
    WHEN td.day_of_week = '0' THEN 'Sunday'
    WHEN td.day_of_week = '1' THEN 'Monday'
    WHEN td.day_of_week = '2' THEN 'Tuesday'
    WHEN td.day_of_week = '3' THEN 'Wednesday'
    WHEN td.day_of_week = '4' THEN 'Thursday'
    WHEN td.day_of_week = '5' THEN 'Friday'
    WHEN td.day_of_week = '6' THEN 'Saturday'
    ELSE 'Invalid Day'
  END AS day_of_week,
    EXTRACT(HOUR FROM t.start_at_timestamp) AS hour_of_day,
    ROUND(AVG(duration_minutes),2) AS average_duration
FROM fact_time_spent t
JOIN time_dimension td ON t.ride_date = td.ride_date
GROUP BY td.day_of_week, hour_of_day
ORDER BY td.day_of_week, hour_of_day;





