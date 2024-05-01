-- Dimension Tables

CREATE TABLE time_dimension (
    ride_date DATE,
    year_of_date INT,
	quarter_of_year INT,
	month_of_date INT,
	day_of_week VARCHAR(15)
);

-- Load data into time_dimension
INSERT INTO time_dimension (ride_date, year_of_date, quarter_of_year, month_of_date, day_of_week)
SELECT DISTINCT
    CAST(start_at AS DATE) AS ride_date,
	YEAR(start_at) AS year_of_date,
	DATEPART(QUARTER, start_at) AS quarter_of_year,
	DATENAME(MONTH, start_at) AS month_of_date,
    DATENAME(DW, start_at) AS day_of_week
FROM dbo.stage_trip;