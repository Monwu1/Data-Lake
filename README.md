# Introduction

Sparkify, a startup company, needs to analyze the data they collected on songs and user activity on the new music streaming app. Sparkify needs to move their data warehouse to a data lake as the database has has grown even more. The data resides in S3, a directory of JSON logs and a directory with JSON metadata. Sparkify's analytics team wants to know what songs users are listening to on the app. 

Data engineer needs to build an ETL pipeline that extracts data from S3, processes them with Spark, and loads the data back into S3 as a set of dimension tables. 

There two files created for this project, etl.py and dl.cfg.



# Summary of Project
### Dimension Tables

Songplays Table

Attributes | Datatype 
------ | ------ 
songplay_id | int 
start_time | timestamp
user_id | int 
level | Varchar
artist_id | Varchar
session_id | int
location | Varchar
user_agent | Varchar


Users Table

Attributes | Datatype 
------ | ------ 
user_id | int 
first_name | Varchar
last_name | Varchar
gender | Varchar
level | Varchar


Songs Table

Attributes | Datatype 
------ | ------ 
song_id | Varchar 
title | Varchar
artist_id | Varchar
year | int
duration | numeric

Artists Table

Attributes | Datatype 
------ | ------ 
artist_id | Varchar 
name | Varchar
location | Varchar
latitude | real
longitude | real

Time Table

Attributes | Datatype 
------ | ------ 
start_time | timestamp 
hour | int
day | int
week | int
month | Varchar
year | int
weekday | Varchar


### Files

* etl.py: This python file builds ETL to load data from S3 and then transfer data to dimension tables on spark, and the dimension tables are loaded back to S3. 

* dl.cfg: This file is configuration file where stores the setting for AWS users access key ID and secret access key.


# User Guide

1. create a user in AWS with access for S3, and enter the access key ID and secret access key in dl.cfg file.
2. run etl.py to load data from S3 to process the data with spark, and transfer the data into dimension tables, then load the data back S3.



