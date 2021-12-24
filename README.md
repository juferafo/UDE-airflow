# UDE-airflow

## Project datasets

As mentioned in the introduction, we are going to process the information present in the song and the log datasets.They encapsulate different information and, therefore, their schema is also different.

### Song dataset

The song dataset is a subset of the [Million Song Dataset](http://millionsongdataset.com/) and it contains information about the songs available in the music app. The records are categorized, among other fields, by artist ID, song ID, title, duration, etc... Each row is written as a JSON with the schema shown below and file organized in folders with the following structure.

```
./data/song_data/
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

The log dataset contains information about the user interaction with the app (sign-in/out, user ID, registration type, listened songs, etc...). This dataset was build from the events simulator [eventsim](https://github.com/Interana/eventsim) and, like the song dataset, the information is stored in JSON files. Below you can find the schema of the log dataset.

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

For convenience the above datasets are placed in the S3 buckets shown below. However, it is also possible to employ the `./data` file if we want to work with this data from local.

* Song data bucket: `s3://udacity-dend/song_data`
* Log data bucket: `s3://udacity-dend/log_data`

#### Redshift star-schema

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
