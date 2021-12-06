# Data Modeling with Postgresql

## Objective
In this project I aimed to extract data from S3, perform ETL and reload the data into S3 for Sparkify, a fictional startup and music streaming platform. They have been collecting user and song activity on their new music streaming app, and have employed the skills of data analysts who are particularly interested in knowing what songs users are listening to. They have grown their user base and song database even more and want to move their data warehouse to a data lake. As such my skills as a data engineer was employed. My role in this project was to create a star schema and an ETL pipeline from both log data and song data.

Briefly:
1. Setup a database from the log and song datasets.

Both datasets are in the json format. The log dataset is derived from [Eventsim](https://github.com/Interana/eventsim). Eventsim is a program that simulates activity logs from a music streaming app for testing and demo purposes. Meanwhile, the song dataset originates from a freely-available collection of audio features and metadata for a million contemporary popular music tracks at [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).

2. Developed an ETL pipeline that pulls, processes data from the log and song datasets, reads processed data to parquet format, and loads it AWS S3.

## Method
In order to attain my objective, I performed the following:

1. Setup an AWS IAM role that grants the necessary access to extract, read and load on AWS S3.
       
2. With the aid of my python script, perform the accomplish the following:
    i.   develop a pipeline that connects to Sparkify's AWS S3 bucket and extract data from S3
    ii.  parse JSON formatted files from there into comprehensive dataframes using *spark*
    iii. more importantly prepare a star schema comprised of: 
            a. fact table: songplays
            b. four dimensional tables: users, songs, artists, and time
    iv.  write the aforementioned dataframes to S3 bucket in *parquet* format.

## Result
This project yielded the following:
1. **dl.cfg** contains necessary IAM_Role configurations.  
2. **etl.py** Extracts datasets from AWS S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.


## Installations and usage
In terminal run **etl.py** as indicated below:
*python etl.py*