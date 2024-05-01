# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating bronze and Gold layers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE db_Bronze;
# MAGIC CREATE DATABASE db_Gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Delta loads from csv (DBFS)

# COMMAND ----------

riders = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("rider_id string, first string, last string, address string, birthday string, account_start_date string, account_end_date string, is_member string").option("sep", ",").load("/FileStore/AzureDbrProject/riders.csv")

payments = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("payment_id string, date string, amount string, rider_id string").option("sep", ",").load("/FileStore/AzureDbrProject/payments.csv")

stations = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("station_id string, name string, latitude string, longitude string").option("sep", ",").load("/FileStore/AzureDbrProject/stations.csv")

trips = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("trip_id string, rideable_type string, started_at  string, ended_at string, start_station_id string, end_station_id string, rider_id string").option("sep", ",").load("/FileStore/AzureDbrProject/trips.csv")

# COMMAND ----------

riders.write.format("delta").mode("overwrite").saveAsTable("db_Bronze.stage_Riders")
payments.write.format("delta").mode("overwrite").saveAsTable("db_Bronze.stage_Payments")
stations.write.format("delta").mode("overwrite").saveAsTable("db_Bronze.stage_Stations")
trips.write.format("delta").mode("overwrite").saveAsTable("db_Bronze.stage_Trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (STAR Schema) Create Scripts

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS db_Gold.fact_time_spent;
# MAGIC DROP TABLE IF EXISTS db_Gold.fact_amount_Spent;
# MAGIC DROP TABLE IF EXISTS db_Gold.time_dimension;
# MAGIC DROP TABLE IF EXISTS db_Gold.station_dimension;
# MAGIC DROP TABLE IF EXISTS db_Gold.rider_dimension;
# MAGIC
# MAGIC
# MAGIC -- Dimension Tables
# MAGIC
# MAGIC CREATE TABLE db_Gold.time_dimension (
# MAGIC     ride_date DATE,
# MAGIC     year_of_date INTEGER,
# MAGIC 	quarter_of_year INTEGER,
# MAGIC 	month_of_date INTEGER,
# MAGIC 	day_of_week VARCHAR(15)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC CREATE TABLE db_Gold.station_dimension (
# MAGIC     station_id VARCHAR(50),
# MAGIC     station_name VARCHAR(75),
# MAGIC     latitude DECIMAL(10,7),
# MAGIC     longitude DECIMAL(10,7)
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE db_Gold.rider_dimension (
# MAGIC     rider_id INTEGER,	
# MAGIC 	account_start_date DATE,
# MAGIC     account_end_date DATE,
# MAGIC     age_at_ride_time INTEGER,
# MAGIC     is_member BOOLEAN
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Fact Table
# MAGIC
# MAGIC CREATE TABLE db_Gold.fact_time_spent (
# MAGIC     ride_id INTEGER,
# MAGIC 	trip_id VARCHAR(50),
# MAGIC     rider_id INTEGER,
# MAGIC     start_station_id VARCHAR(50),
# MAGIC     end_station_id VARCHAR(50),
# MAGIC 	rideable_type VARCHAR(75),
# MAGIC     duration_minutes INTEGER,
# MAGIC 	start_at_timestamp TIMESTAMP,
# MAGIC     ride_date DATE
# MAGIC );
# MAGIC
# MAGIC
# MAGIC CREATE TABLE db_Gold.fact_amount_Spent (
# MAGIC 	pay_ride_id INTEGER,
# MAGIC     payment_id INTEGER,
# MAGIC     rider_id INTEGER,
# MAGIC 	payment_date DATE,
# MAGIC     amount DECIMAL(10,7)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Star Schema) Insert Scripts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO db_Gold.rider_dimension (rider_id, account_start_date, account_end_date, is_member, age_at_ride_time)
# MAGIC SELECT DISTINCT
# MAGIC     CAST(rider_id as INT),
# MAGIC     CAST(account_start_date AS DATE),
# MAGIC     CAST(account_end_date AS DATE),
# MAGIC     CAST(is_member AS BOOLEAN),
# MAGIC     COALESCE(YEAR(CAST(account_start_date AS DATE)),0) - COALESCE(YEAR(CAST(birthday AS DATE)),0) AS age_at_ride_time
# MAGIC FROM db_Bronze.stage_riders;
# MAGIC
# MAGIC -- Load data into station_dimension
# MAGIC INSERT INTO db_Gold.station_dimension (station_id, station_name, latitude, longitude)
# MAGIC SELECT DISTINCT
# MAGIC     station_id,
# MAGIC     name AS station_name,
# MAGIC     CAST(latitude as DECIMAL(10,7)) as latitude,
# MAGIC     CAST(longitude as DECIMAL(10,7)) as longitude
# MAGIC FROM db_Bronze.stage_stations;
# MAGIC
# MAGIC -- Load data into time_dimension
# MAGIC INSERT INTO db_Gold.time_dimension (ride_date, year_of_date, quarter_of_year, month_of_date, day_of_week)
# MAGIC SELECT DISTINCT
# MAGIC    DATE(started_at) AS ride_date,
# MAGIC 	YEAR(started_at) AS year_of_date,
# MAGIC 	QUARTER(started_at) AS quarter_of_year,
# MAGIC 	MONTH(started_at) AS month_of_date,
# MAGIC   date_format(started_at, 'EEEE') AS day_of_week
# MAGIC FROM db_Bronze.stage_trips;
# MAGIC
# MAGIC -- Load data into ride_fact
# MAGIC INSERT INTO db_Gold.fact_time_spent (ride_id, trip_id, rider_id, start_station_id, end_station_id, rideable_type, duration_minutes, start_at_timestamp, ride_date)
# MAGIC SELECT
# MAGIC   row_number() OVER (order by 1) as ride_id,
# MAGIC   CAST(t.trip_id as VARCHAR(50)) as trip_id,
# MAGIC   CAST(t.rider_id as INT) as rider_id,
# MAGIC   t.start_station_id,
# MAGIC   t.end_station_id,
# MAGIC   t.rideable_type,
# MAGIC   ROUND(timestampdiff(SECOND, t.ended_at, t.started_at) / 60,2) AS duration_minutes,
# MAGIC   CAST(t.started_at as TIMESTAMP)  AS start_at_timestamp,
# MAGIC   CAST(t.started_at as DATE) AS ride_date
# MAGIC FROM db_Bronze.stage_trips t
# MAGIC JOIN db_Gold.time_dimension td ON DATE(t.started_at) = td.ride_date
# MAGIC JOIN db_Gold.station_dimension ss ON t.start_station_id = ss.station_id
# MAGIC JOIN db_Gold.station_dimension es ON t.end_station_id = es.station_id
# MAGIC JOIN db_Gold.rider_dimension rd ON t.rider_id = rd.rider_id;
# MAGIC
# MAGIC
# MAGIC -- Load data into payment_dimension
# MAGIC INSERT INTO db_Gold.fact_amount_Spent (pay_ride_id, payment_id, rider_id, payment_date, amount)
# MAGIC SELECT DISTINCT
# MAGIC   ROW_NUMBER() OVER (ORDER BY 1) AS pay_ride_id,
# MAGIC 	CAST(p.payment_id as INT) as payment_id,
# MAGIC 	r.rider_id,
# MAGIC   CAST(p.date as DATE) as payment_date,
# MAGIC   CAST(p.amount AS DECIMAL(10,7)) as amount
# MAGIC FROM db_Bronze.stage_payments p 
# MAGIC JOIN db_Gold.rider_dimension r
# MAGIC on CAST(p.rider_id AS INT) = r.rider_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a. Based on date and time factors such as day of the week and time of day
# MAGIC SELECT
# MAGIC     day_of_week,
# MAGIC     EXTRACT(HOUR FROM t.start_at_timestamp) AS hour_of_day,
# MAGIC     ROUND(AVG(duration_minutes),2) AS average_duration
# MAGIC FROM db_Gold.fact_time_spent t
# MAGIC JOIN db_Gold.time_dimension td 
# MAGIC ON t.ride_date = td.ride_date
# MAGIC GROUP BY td.day_of_week, hour_of_day
# MAGIC ORDER BY td.day_of_week, hour_of_day;

# COMMAND ----------

# MAGIC %sql
# MAGIC --b. Based on which station is the starting and/or ending station
# MAGIC SELECT
# MAGIC     ss.station_name AS start_station,
# MAGIC     es.station_name AS end_station,
# MAGIC     ROUND(AVG(duration_minutes),2) AS average_duration
# MAGIC FROM db_Gold.fact_time_spent t
# MAGIC JOIN db_Gold.station_dimension ss ON t.start_station_id = ss.station_id
# MAGIC JOIN db_Gold.station_dimension es ON t.end_station_id = es.station_id
# MAGIC GROUP BY start_station, end_station
# MAGIC having ss.station_name <> es.station_name
# MAGIC ORDER BY start_station, end_station;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c. Based on the age of the rider at the time of the ride
# MAGIC SELECT
# MAGIC     rd.age_at_ride_time,
# MAGIC     AVG(duration_minutes) AS average_duration
# MAGIC FROM db_Gold.fact_time_spent t
# MAGIC JOIN db_Gold.rider_dimension rd ON t.rider_id = rd.rider_id
# MAGIC GROUP BY rd.age_at_ride_time
# MAGIC ORDER BY rd.age_at_ride_time;

# COMMAND ----------

# MAGIC %sql
# MAGIC --d. Based on whether the rider is a member or a casual rider
# MAGIC SELECT
# MAGIC     rd.is_member,
# MAGIC     ROUND(AVG(duration_minutes),2) AS average_duration
# MAGIC FROM db_Gold.fact_time_spent t
# MAGIC JOIN db_Gold.rider_dimension rd ON t.rider_id = rd.rider_id
# MAGIC GROUP BY rd.is_member;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2a. Per month, quarter, year
# MAGIC SELECT
# MAGIC     EXTRACT(YEAR FROM payment_date) AS payment_year,
# MAGIC     EXTRACT(QUARTER FROM payment_date) AS payment_quarter,
# MAGIC     EXTRACT(MONTH FROM payment_date) AS payment_month,
# MAGIC     SUM(amount) AS total_amount
# MAGIC FROM db_Gold.fact_amount_spent
# MAGIC GROUP BY payment_year, payment_quarter, payment_month
# MAGIC ORDER BY payment_year, payment_quarter, payment_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2b. Per member, based on the age of the rider at account start (Check for the age again)
# MAGIC SELECT
# MAGIC     rd.age_at_ride_time,
# MAGIC     rd.is_member,
# MAGIC     SUM(amount) AS total_amount
# MAGIC FROM db_Gold.fact_amount_spent p
# MAGIC JOIN db_Gold.rider_dimension rd ON p.rider_id = rd.rider_id
# MAGIC GROUP BY rd.age_at_ride_time, rd.is_member
# MAGIC ORDER BY rd.age_at_ride_time, rd.is_member;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 3

# COMMAND ----------

# MAGIC %sql
# MAGIC --3a. Based on how many rides the rider averages per month
# MAGIC SELECT
# MAGIC     rd.rider_id,
# MAGIC     COUNT(*) AS total_rides,
# MAGIC     ROUND(AVG(amount),2) AS average_amount
# MAGIC FROM db_Gold.fact_amount_spent p
# MAGIC JOIN db_Gold.rider_dimension rd ON p.rider_id = rd.rider_id
# MAGIC JOIN db_Gold.fact_time_spent t ON rd.rider_id = t.rider_id
# MAGIC GROUP BY rd.rider_id
# MAGIC ORDER BY total_rides DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --3b. Based on how many minutes the rider spends on a bike per month
# MAGIC SELECT
# MAGIC     rd.rider_id,
# MAGIC     EXTRACT(MONTH FROM t.ride_date) AS ride_month,
# MAGIC     SUM(duration_minutes) AS total_duration,
# MAGIC     ROUND(AVG(amount),2) AS average_amount
# MAGIC FROM db_Gold.fact_time_spent t
# MAGIC JOIN db_Gold.rider_dimension rd ON t.rider_id = rd.rider_id
# MAGIC JOIN db_Gold.fact_amount_spent p ON t.rider_id = p.rider_id
# MAGIC GROUP BY rd.rider_id, ride_month
# MAGIC ORDER BY rd.rider_id, ride_month;
