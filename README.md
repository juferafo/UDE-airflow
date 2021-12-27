# UDE-airflow

In previous repositories we have presented ilustrated real life scenarios in the context of the Data Engineering. We have explored the normalization and denormalization of data-sets and how these models fit in [SQL (PostgreSQL)](https://github.com/juferafo/UDE-postgres) and [no-SQL (Casandra)](https://github.com/juferafo/UDE-cassandra) models, the underlying structure of [Data Warehouses (Redshift)](https://github.com/juferafo/UDE-redshift) (DWH) and [Data Lakes (S3)](https://github.com/juferafo/UDE-data-lake) (DL) and why it is useful to make use of cloud providers to host these resources. 

Following the line of these repositories we present here the use case where our company (Sparkify) needs to feed the Data Warehouse hosted in [Redshift](https://aws.amazon.com/redshift/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) with fresh data in a regular basis. 

To acompish this task we will employ [Apache Airflow](https://airflow.apache.org/). Apache Airflow is an open-source orquestration tool that is widely used to configure and trigger data pipelines both on a regular basis and on demand. In our case, the pipeline will read fresh data from the staging area located in Amazon S3, perform transformations on it if needed, and insert the data in Redshift where the information is organized as a [star-shaped schema](https://www.guru99.com/star-snowflake-data-warehousing.html). 

## Project datasets

We are going to process data related to the customer's usage of a music app. This information is organized in two datasets: the song and the log dataset as they encapsulate different particularities.

### Song dataset

The song dataset is a subset of the [Million Song Dataset](http://millionsongdataset.com/) and it contains information about the songs available in the music app. The records are categorized, among other fields, by artist ID, song ID, title, duration, etc... Each row is written as a JSON file with the following schema and file structure.

```
/PATH/TO/song_data/
└── A
    ├── A
    │   ├── A
    │   ├── B
    │   └── C
    └── B
        ├── A
        ├── B
        └── C
```

Example of a song data file.

```
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

### Log dataset

The log dataset contains information about the user interaction with the app (sign-in/out, user ID, registration type, listened songs, etc...). This dataset was built from the events simulator [eventsim](https://github.com/Interana/eventsim) and, like the song dataset, the information is stored in JSON files. Below you can find the schema of the log dataset.

```
{
  artist TEXT,
  auth TEXT,
  firstName TEXT,
  gender TEXT,
  itemInSession INT,
  lastName TEXT,
  length DOUBLE,
  level TEXT,
  location TEXT,
  method TEXT,
  page TEXT,
  registration DOUBLE,
  sessionId INT,
  song TEXT,
  status INT,
  ts FLOAT,
  userId INT
}
```

The above datasets are placed in the S3 buckets shown below.

* Song data bucket: `s3://udacity-dend/song_data`
* Log data bucket: `s3://udacity-dend/log_data`

## Redshift star-schema

##### `songplays` fact table

```
songplay_id INT,
start_time TIMESTAMP, 
user_id INT, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id INT, 
location VARCHAR, 
user_agent VARCHAR
```

##### `artist` dimension table

```
artist_id VARCHAR,
artist_name VARCHAR,
artist_location VARCHAR,
artist_latitude INT,
artist_longitude INT
```

#### `song` dimension table

```
song_id VARCHAR,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration FLOAT
```

##### `time` dimension table

```
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
```

##### `user` dimension table

```
user_id INT,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
```

### DAG pipeline

### Requirements

1. Apache Airflow
2. Python 3
