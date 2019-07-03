## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL. 

And complex business logics can be easily solved using the STAR schema method.


Create a STAR schema, optimized for song play analysis.
* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time table. 

This database can help determine what song or what artist a particular user likes. We can also determine which agent all our users mainly use and we can use that information to optimize the UI for that specific agent.

* If a user likes songs from a particular artist, then we can determine what songs to suggest based on artist.  
* Recent listened to songs: By joining songplays and user table can show recommendation on the app based on subscription level. 
* Can help in recommending most popular songs of the day/week/month/year.

## ETL
1. Created **songs**, **artist** dimension tables from extracting songs_data by selected columns.
2. Created **users**, **time** dimension tables from extracting log_data by selected columns.
3. Created the **songplays** fact table from the dimensison tables and log_data. 

## Installation

Install PostgreSQL database drivers by using the below command
```bash
pip install psycopg2
```
## execute files in the below order each time before pipeline.

   1. create_tables.py
      ```python
         $ python3 create_tables.py
   2. etl.ipynb/et.py
      ```python
         $ python3 etl.py