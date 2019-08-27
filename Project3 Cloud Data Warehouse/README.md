## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Data Warehouse STAR Schema Desgin

<b>Staging Tables</b>
    * staging_events
    * staging_songs

<b>Fact Table</b>
    * fact_songplay
    
<b>Dim Tables</b>
    * dim_user
    * dim_song
    * dim_artist
    * dim_time
    
## How to Run

1. Run create_redshift_cluster.py (make sure you wait until the cluster is ready before moving on to next step)
```python
$ python create_redshift_cluster.py
```

2. Run create_table.py
```python
$ python create_table.py
```

3. Run etl.py
```python
$ python etl.py
```

4. Run get_count_all_tables.py this will get the count of all the tables created in our redshift cluster
```python
$ python get_count_all_tables.py
```

5. Once you are done you can run delete_redshift_cluster.py to Delete the redshift cluster and remove resources
```python
$ python delete_redshift_cluster.py
```
