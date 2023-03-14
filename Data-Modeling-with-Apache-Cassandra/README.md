# Project 2: Data Modeling with Apache Cassandra

## Dataset Information
**Dataset**: event_data
The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
>           event_data/2018-11-08-events.csv
>           event_data/2018-11-09-events.csv

## Steps Done in this project
1. Use the **event_datafile_new.csv** dataset to create a denormalized dataset. 
2. Modeling the data tables keeping in mind the below queries to run
    * artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
    * name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
    * Every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
3. Loading the data into tables created using Apache Cassandra (CQL)
4. Test the table creation and data insertion using select queries on which the data base tables are built on

#### Schema for table created using Query 1
**Table Name:** music_app_history_by_session_item 
**MetaData:** sessionId INT, artist TEXT, song TEXT, length FLOAT, itemInSession INT, PRIMARY KEY (sessionId, itemInSession)
#### Schema for table created using Query 2
**Table Name:** music_app_history_by_user_session 
**MetaData:** userId INT,sessionId INT,itemInSession INT,artist TEXT,song TEXT,firstName TEXT,lastName TEXT,PRIMARY KEY (userId, sessionid, itemInSession)
#### Schema for table created using Query 3
**Table Name:** music_app_history_by_song 
**MetaData:** song TEXT, firstname TEXT, lastname TEXT, PRIMARY KEY(song)

## Files in Project workspace

* **ETL_Cassandra.ipynb** reads and processes files from event_data folder and subfolders in a streamlined fashion (CSV format), creates and loads data into tables and validates them using select below select statements 
    * "select artist, song, length from music_app_history_by_session_item where sessionId = 338 and itemInSession = 4"
    * "select artist, song, firstname, lastname from music_app_history_by_user_session where userId = 10 and sessionId = 182"
    * "select firstname, lastname from music_app_history_by_song where song = 'All Hands Against His Own'"

* **requirements.txt** has the required dependencies. 
    Created this file in a local virtual environment using 
    >       pip freeze --local > requirements.txt
    Use below command to install dependencies of the project in your local branch:
    >       pip install -r requirements.txt
* **README.md** provides discussion on my project.

## References
* [pip freeze](https://stackoverflow.com/questions/54931275/pip-freeze-local)
* [Markdown Guide](https://www.markdownguide.org/basic-syntax/#paragraphs-1)