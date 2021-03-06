# UDE-airflow

In previous repositories we have presented real life scenarios in the context of the Data Engineering like the normalization and denormalization of data-sets and how these models fit in [SQL (PostgreSQL)](https://github.com/juferafo/UDE-postgres) and [no-SQL (Casandra)](https://github.com/juferafo/UDE-cassandra) models, the underlying structure of [Data Warehouses (Redshift)](https://github.com/juferafo/UDE-redshift) (DWH) and [Data Lakes (S3)](https://github.com/juferafo/UDE-data-lake) (DL) and why it is advantageous to make use of cloud providers to host these resources. 

In the line of these repositories we present here the following use case: our company (Sparkify) needs to feed the Data Warehouse hosted in [Redshift](https://aws.amazon.com/redshift/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) with fresh data in a regular basis. To accomplish this task we will employ [Apache Airflow](https://airflow.apache.org/) that is a  widely used open-source orchestration tool employed to configure and trigger data pipelines both on a regular basis (daily, weekely) and on demand. In our case, the pipeline will read fresh data from the located in Amazon S3, perform transformations on it (if needed), and insert the data in Redshift where the information is organized as a [star-shaped schema](https://www.guru99.com/star-snowflake-data-warehousing.html). 

## Project datasets

We are going to process data related to the customer's usage of a music app. This information is organized in two datasets: the song and the log dataset as they encapsulate different particularities.

### Song dataset

The song dataset is a subset of the [Million Song Dataset](http://millionsongdataset.com/) and it contains information about the songs available in the music app. Among other fields, the records are categorized by: artist ID, song ID, title, duration, etc... Each row is written as a JSON file with the following schema and file structure.

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

The log dataset contains information about the user interaction with the app (sign-in/out, user ID, registration type, connection string, listened songs, etc...). This dataset was built from the events simulator [eventsim](https://github.com/Interana/eventsim) and, like the song dataset, the information is stored in JSON files. Below you can find the schema of the log dataset.

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

## Redshift structure

As mentioned above, Redshift will be the end place of the log and song data. This information will be re-shaped in the form of a star-schema where `songplays` will be the fact table that encapsulates the songs played by the customers and `artist`, `song`, `time` and `user` provide additional details to certain particularities of the data in the fact table. Below one can find the schema of each of these tables.


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

## Data pipeline

As mentioned in the introduction, Apache Airflow is an orchestration tool employed to trigger ETL pipelines at a given schedule. These pipelines group together a set of tasks like, for example, execute a query against a database. The pipelines must be designed without any loop and, because of this particularity, they are known as Diagrammatic Acyclic Graphs or DAGs. The pipelines are coded in Python and each task is instantiated as an [Airflow Operator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/index.html). Despite of the wide range of operators already included in the [Airflow library](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html) it is possible to design a customized one or to add new features to an already existing operator to suit the user's needs.

### DAG pipeline

The DAG can be found in the script `./dags/sparkify_dag.py` and below you can find the DAG pipeline from `Start` to `End`.

![dag](./figs/dag.png)

#### Custom operators

Four [custom operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html) were designed to make the DAG code easier to understand:

1. `StageToRedshiftOperator`: this operator was coded to copy data from S3 into Redshift.

2. `LoadFactOperator`: this operator is used to load data into the fact table `songplays`.

3. `LoadDimensionOperator`: this operator is used to load data into the dimension tables `artists`, `songs`, `time` and `users`.

4. `DataQualityOperator`: this operator is used to check the quality of the data loaded into Redshift.

The code of these operators and additional helper classes can be found in `./plugins/operators` and `./plugins/helpers/sql_queries.py`.

### Initial set-up configuration

Before testing this repository it is necessary to go through the below steps:

1. Redshift cluster: since the data will be ingested into a DWH hosted in Redshift we need to create a cluster and make it accessible so we can insert such data. Taking a look into the pipeline one can notice that there are no steps for creating or dropping tables, therefore, the user must create the tables before running the pipeline. To do so, one can employ the SQL DDL statements in `./create_tables.sql`.
2. The Airflow [connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) `aws_credentials` must be configured as `Amazon Web Services` and it will contain the AWS credentials required, for example, to read data from S3.
3. The Airflow connection `redshift_conn_id` must be configured as `Postgres`. It will encapsulate the endpoint, schema, login and password to access the Redshift cluster.

## Requirements

1. [Apache Airflow](https://airflow.apache.org/)
2. [Python 3](https://www.python.org/)
