# Data-Engineering-Projects

## Project 1

Link: [Data-Modeling-with-Postgres](https://github.com/uhkm/Data-Engineering-Projects/tree/main/Data-Modeling-with-Postgres)
> This is a project for Sparkify (a simulated start-up). 
> Sparkify wants to analyze the data they have collected on their music streaming app. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. For this project, I built an ETL pipeline using python with data modeling done in Postgres and used the query files given by them to compare my results.

## Project 2

Link: [Data-Modeling-with-Postgres](https://github.com/uhkm/Data-Engineering-Projects/tree/main/Data-Modeling-with-Apache-Cassandra)
> This project is aimed at implementing data modeling and building an ETL pipeline in Python using the cassandra-driver for Sparkify, a simulated start-up. The data is sourced from CSV files, and cassandra tables are created based on the queries used for analysis. Below are the queries that was used to build the database around:
  * Give the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
  * Give only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
  * Give every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
