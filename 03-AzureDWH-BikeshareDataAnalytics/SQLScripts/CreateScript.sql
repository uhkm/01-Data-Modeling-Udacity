DROP TABLE IF EXISTS fact_time_spent;
DROP TABLE IF EXISTS fact_amount_Spent;
DROP TABLE IF EXISTS time_dimension;
DROP TABLE IF EXISTS station_dimension;
DROP TABLE IF EXISTS rider_dimension;


-- Dimension Tables

CREATE TABLE time_dimension (
    ride_date DATE PRIMARY KEY,
    year_of_date INTEGER,
	quarter_of_year INTEGER,
	month_of_date INTEGER,
	day_of_week VARCHAR(15)
);


CREATE TABLE station_dimension (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(75),
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE rider_dimension (
    rider_id INTEGER PRIMARY KEY,	
	account_start_date DATE,
    account_end_date DATE,
    age_at_ride_time INTEGER,
    is_member BOOLEAN
);



-- Fact Table

CREATE TABLE fact_time_spent (
    ride_id SERIAL PRIMARY KEY,
	trip_id VARCHAR(50) REFERENCES trip(trip_id),
    rider_id INTEGER REFERENCES rider_dimension(rider_id),
    start_station_id VARCHAR(50) REFERENCES station_dimension(station_id),
    end_station_id VARCHAR(50) REFERENCES station_dimension(station_id),
--     payment_id INTEGER REFERENCES payment(payment_id),
	rideable_type VARCHAR(75),
    duration_minutes INTEGER,
	start_at_timestamp TIMESTAMP WITHOUT TIME ZONE,
    ride_date DATE
);


CREATE TABLE fact_amount_Spent (
	pay_ride_id SERIAL PRIMARY KEY,
    payment_id INTEGER REFERENCES payment(payment_id),
    rider_id INTEGER REFERENCES rider_dimension(rider_id),
	payment_date DATE,
    amount MONEY
);






