# Sparkify S3 to Redshift ETL Pipeline (Documentation)

***

The following pipeline showcases the following concepts:
   * Pulling data from S3 into stage tables within Redshift
   * Dimensional modeling data from stage tables into fact/dimension tables
   

## Context
***

> A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

> As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Given the needs of the analytics team, the data engineering team and I have developed a ETL pipeline that feeds song and log data from our S3 into Redshift, a cloud data warehouse. Due to the formatting of the raw data within S3, we had to create staging tables for both datasets. Once in Redshift, we then modeled the two stage tables into a series of dimension tables and a fact table, capturing songs listened based on user and starttime. This created a usable data warehouse for the analytics team to utilize during their analyses. As a result, we've developed a standardized repository for people to access and query data, fulfilling the task at hand.



## Data
***

**Event Dataset:**
> This data set consists of information about events done by users

**Field Names**: artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId


<br/>

**Song Dataset:**
> This data set consists of information about artist and their songs

**Field Names**: song_id, num_songs, title, artist_name, artist_latitude, year, duration, artist_id, artist_longitude, artist_location



## Data Schema
***

With the two datasets stored on a S3 instance, we had to figure out how we could get it from storage to a platform that we can work with it. Having chosen Redshift as our platform of choice, we decided to design a staging area for us to load data into, to then use to model the necessary dimension tables and fact table for our data warehousing purposes.

The first step was to COPY the files within S3 over to the staging tables that we had defined. From there, we performed insertion statements that pulled (combination of) data from the two tables into the appropriate dimension tables, creating a fact table to tie everything together.

As a result, we've designed a data warehouse that stored both song and log information in one centralized place. This will serve as a repository for those within the anayltics team to pull data from and to perform necessary queries for further analysis. The design of the schema is presented below.

***
**Stage Tables**

   * **stage_events**
      * **artist          (text)**
      * **auth            (text)** 
      * **firstName       (text)**
      * **gender          (text)**   
      * **itemInSession   (int)**
      * **lastName        (text)**
      * **length          (float)**
      * **level           (text)** 
      * **location        (text)**
      * **method          (text)**
      * **page            (text)**
      * **registration    (bigint)**
      * **sessionId       (int)**
      * **song            (text)**
      * **status          (int)**
      * **ts              (timestamp)**
      * **userAgent       (text)**
      * **userId          (int)**
      
      
   * **stage_songs**
      * **song_id          (text)**
      * **num_songs        (int)**
      * **title            (text)**
      * **artist_name      (text)**
      * **artist_latitude  (float)**
      * **year             (int)**
      * **duration         (float)**
      * **artist_id        (text)**
      * **artist_longitude (float)**
      * **artist_location  (text)**

***
**Fact Table**


   * **songPlays:**
       * **songplay_id (int)  PRIMARY KEY IDENTITY(0,1)**: Unique ID for a songplay record
       * **start_time  (text) SORTKEY DISTKEY**: Start time when an event occured
       * **user_id     (int)  REFERENCES users(user_id)**: Unique ID for a user
       * **level       (text)**: Level of the account, on whether the user paid
       * **song_id     (text) REFERENCES users(song_id)**: Unique ID for a song record
       * **artist_id   (text) REFERENCES users(artist_id)**: Unique ID for an artist record
       * **session_id  (int)**: Unique ID for a session
       * **location    (text)**: Location when the event occured
       * **user_agent  (text)**: The user agent that the user used
       
***
**Dimension Tables**

   * **users**: **(diststyle all)**
       * **user_id     (int)  PRIMARY KEY SORTKEY**: Unique ID for a user record
       * **first_name  (text)**: First name of user
       * **last_name   (text)**: last name of user
       * **gender      (text)**: Gender of the user
       * **level       (text)**: Level of the account, on whether the user paid
       
       
   * **songs**: **(diststyle all)**
       * **song_id     (text) PRIMARY KEY SORTKEY**: Unique ID for a song record
       * **title       (text)**: Title of a song
       * **artist_id   (text)**: Unique ID for a artist record
       * **year        (int)**: Year that the song was released
       * **duration    (float)**: The duration of the song
       
       
   * **artist**: **(diststyle all)**
       * **artist_id   (text) PRIMARY KEY SORTKEY**: Unique ID for an artist record
       * **name        (text)**: Name of artist
       * **location    (text)**: Location of the artist
       * **latitude    (float)**: Latitude of the artist's location
       * **longitute   (float)**: longitute of the artist's location
       
       
   * **times**:
       * **start_time  (text) PRIMARY KEY SORTKEY DISTKEY**: Start time when an event occured
       * **hour        (int)**: Hour of event occurance
       * **day         (int)**: Day of event occurance
       * **week        (int)**: Week of event occurance
       * **month       (int)**: Month of event occurance
       * **year        (int)**: Year of event occurance
       * **weekday     (int)**: Weekday of event occurance
    

In terms of the choices that we've made in designing this schema, we decided to distribute all dimension tables (besides time) in a `diststyle all` fashion to allow efficient joining to be done on each CPU that we had running. To further optimize reads, we've also set certain tables to be sorted base on ID, allowing for better scans for a particular ID. For bigger tables such as `times`, we came to decide to distribute it based on key, allowing similar values to be stored on the same CPU to mitigate shuffling across multiple CPUs.
       
## File Structure

***

   * **sql_queries.py** - Defines the database schema, from the staging tables to the dimension and fact tables.
   * **create_tables.py** - Generates tables within Redshift, creating and dropping tables
   * **etl.py** - Copy data from S3 into tables within Redshift, inserting data from stage tables into dimension and fact tables.
   * **dwh.cfg** - Specifies global parameters used throughout the ETL pipeline.
   
   
## How to run ETL Pipeline

***
Assuming that a Redshift cluster is up and running, and `dwh.cfg` is filled with the appropriate parameters, the first step of the ETL process is to run `create_tables.py` to generate tables within the cloud service. The next subsequent step is to run `etl.py` to load data from S3 into the stage tables within Redshift to then insert into the defined dimension and fact tables. As the end result, the pipeline migrated data from S3 into Redshift, conforming the raw data into a star schema, to be used as a data warehouse for our analytics team.
