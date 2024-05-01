# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating bronze and Gold layers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS db_Silver;
# MAGIC CREATE DATABASE IF NOT EXISTS db_Gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Delta loads from csv (DBFS)

# COMMAND ----------

riders = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("rider_id string, first string, last string, address string, birthday string, account_start_date string, account_end_date string, is_member string").option("sep", ",").load("/FileStore/AzureDbrProject/riders.csv")

payments = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("payment_id string, date string, amount string, rider_id string").option("sep", ",").load("/FileStore/AzureDbrProject/payments.csv")

stations = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("station_id string, name string, latitude string, longitude string").option("sep", ",").load("/FileStore/AzureDbrProject/stations.csv")

trips = spark.read.format("csv").option("inferSchema", "false").option("header", "false").schema("trip_id string, rideable_type string, started_at  string, ended_at string, start_station_id string, end_station_id string, rider_id string").option("sep", ",").load("/FileStore/AzureDbrProject/trips.csv")

# COMMAND ----------

riders.write.format("delta").mode("overwrite").save("/delta/bronze_Riders")
payments.write.format("delta").mode("overwrite").save("/delta/bronze_Payments")
stations.write.format("delta").mode("overwrite").save("/delta/bronze_Stations")
trips.write.format("delta").mode("overwrite").save("/delta/bronze_Trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating loads from delta files (Silver layer)

# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS db_Silver.stage_Riders""")
spark.sql("""CREATE TABLE db_Silver.stage_Riders USING DELTA LOCATION '/delta/bronze_Riders'""")
spark.sql(""" DROP TABLE IF EXISTS db_Silver.stage_Payments""")
spark.sql("""CREATE TABLE db_Silver.stage_Payments USING DELTA LOCATION '/delta/bronze_Payments'""")
spark.sql(""" DROP TABLE IF EXISTS db_Silver.stage_Stations""")
spark.sql("""CREATE TABLE db_Silver.stage_Stations USING DELTA LOCATION '/delta/bronze_Stations'""")
spark.sql(""" DROP TABLE IF EXISTS db_Silver.stage_Trips""")
spark.sql("""CREATE TABLE db_Silver.stage_Trips USING DELTA LOCATION '/delta/bronze_Trips'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating gold layer tables using saveAsTable method

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS db_Gold.fact_time_spent;
# MAGIC DROP TABLE IF EXISTS db_Gold.fact_amount_Spent;
# MAGIC DROP TABLE IF EXISTS db_Gold.time_dimension;
# MAGIC DROP TABLE IF EXISTS db_Gold.station_dimension;
# MAGIC DROP TABLE IF EXISTS db_Gold.rider_dimension;

# COMMAND ----------

# Dimension Tables
# Load data into rider_dimension
spark.sql("""SELECT DISTINCT
    CAST(rider_id as INT),
    CAST(account_start_date AS DATE),
    CAST(account_end_date AS DATE),
    CAST(is_member AS BOOLEAN),
    COALESCE(YEAR(CAST(account_start_date AS DATE)),0) - COALESCE(YEAR(CAST(birthday AS DATE)),0) AS age_at_ride_time
FROM db_Silver.stage_riders""").write.format("delta").mode("overwrite").saveAsTable("db_Gold.rider_dimension")


# Load data into station_dimension
spark.sql("""SELECT DISTINCT
    CAST(station_id as VARCHAR(50)) as station_id,
    CAST(name as VARCHAR(50)) AS station_name,
    CAST(latitude as DECIMAL(10,7)) as latitude,
    CAST(longitude as DECIMAL(10,7)) as longitude
FROM db_Silver.stage_stations""").write.format("delta").mode("overwrite").saveAsTable("db_Gold.station_dimension")

# Load data into time_dimension
spark.sql("""SELECT DISTINCT
    DATE(started_at) AS ride_date,
	CAST(YEAR(started_at) AS INT) AS year_of_date,
	CAST(QUARTER(started_at) AS INT) AS quarter_of_year,
	CAST(MONTH(started_at) AS INT) AS month_of_date,
    date_format(started_at, 'EEEE') AS day_of_week
FROM db_Silver.stage_trips""").write.format("delta").mode("overwrite").saveAsTable("db_Gold.time_dimension")


# Fact Table
# Load data into fact_time_spent
spark.sql("""SELECT
  row_number() OVER (order by 1) as ride_id,
  CAST(t.trip_id as VARCHAR(50)) as trip_id,
  CAST(t.rider_id as INT) as rider_id,
  CAST(t.start_station_id as VARCHAR(50)) as start_station_id,
  CAST(t.end_station_id as VARCHAR(50)) as end_station_id,
  CAST(t.rideable_type as VARCHAR(75)) as rideable_type,
  CAST(ROUND(timestampdiff(SECOND, t.ended_at, t.started_at) / 60,2) AS DECIMAL(10,7)) AS duration_minutes,
  CAST(t.started_at as TIMESTAMP)  AS start_at_timestamp,
  CAST(t.started_at as DATE) AS ride_date
FROM db_Silver.stage_trips t
JOIN db_Gold.time_dimension td ON DATE(t.started_at) = td.ride_date
JOIN db_Gold.station_dimension ss ON t.start_station_id = ss.station_id
JOIN db_Gold.station_dimension es ON t.end_station_id = es.station_id
JOIN db_Gold.rider_dimension rd ON t.rider_id = rd.rider_id""").write.format("delta").mode("overwrite").saveAsTable("db_Gold.fact_time_spent")


# Load data into fact_amount_Spent
spark.sql("""SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY 1) AS pay_ride_id,
    CAST(p.payment_id as INT) as payment_id,
    CAST(r.rider_id as INT) as rider_id,
  CAST(p.date as DATE) as payment_date,
  CAST(p.amount AS DECIMAL(10,7)) as amount
FROM db_Silver.stage_payments p 
JOIN db_Gold.rider_dimension r
on CAST(p.rider_id AS INT) = r.rider_id""").write.format("delta").mode("overwrite").saveAsTable("db_Gold.fact_amount_Spent")


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

# COMMAND ----------


