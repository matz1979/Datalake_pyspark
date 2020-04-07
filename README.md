# Datalake project for a music streaming platform

The startup has currently saved their data in a AWS S3 server as JSON files.
As the startup grow the Redshift Data Warehouse now no make sense so we move to a Datalake hosted
on AWS S3 and build an ETL pipeline that extracts their data from S3 and save them back
to the S3 bucket by using Spark.
So that your Data team can still quickly extract the data and analyze the behaviors of the users
from past data and the actual data

## Why I choice the AWS S3 Datalake

The APP generate always the same data and save them to the S3 bucket.
New data can be load easy into the new analytics tables with the ETL pipeline
also the data team can extract and analyze the data with simple SQL queries.

## How to use and explain the files

* Fill the ```dl.cfg``` with your AWS user data.

* Run the ```etl.py``` to load the data from the S3 bucket into the analytics tables
  and save them back to the S3 bucket as a Spark .parquet file

## The Schema of the analytics tables

In this schema for the Datalake is the fact table songplays and it has four dimension tables
all the tables are stored as a Spark parquet files:

* songplays.parquet were stored all the data and some facts
 (song_id, user_id, artist_id, ...) partitioned by year and month

* time.parquet were stored all time data (hour, day, month, ...)
  partitioned by year and month

* users.parquet were stored all user data (first name, last name, ...)

* songs.parquet were stored all song data (title, year, ...) partitioned by year and artist

* artist.parquet were stored all artist data (name, location, ...)
