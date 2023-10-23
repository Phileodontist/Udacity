# Sparkify S3 to Spark to S3 ETL Pipeline (Documentation)

***

The following pipeline showcases the following concepts:
* Reading data from S3, processing data via Spark, and writing back to S3
* Using EMR as a processing platform
* Utilizing parquet as a way of storing data within a data lake
 

## Context
***

>A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

>As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## How to run ETL Pipeline
***
The following pipeline assumes that its within a AWS EMR cluster as its enviroment, reading from a valid S3 bucket, and writing into another valid S3 bucket. As well as, configured accordingly to have the right permissions to access S3 and utilize EMR's processing capabilities. 

Once all the following dependancies are set up, running the pipeline starts with the following command: `spark-submit etl.py`

When the process starts, the ETL script pulls data from the source S3 bucket, reads and transforms song and log data into appropriate fact and dimension tables, in which then is stored in the target S3 bucket.

As a result, each table is stored in it's own directoy, having larger tables partitioned accordingly. With this format, analysts are able to pull data based on their needs more efficiently, relying the parititioning of the data to aid their queries.

![](https://github.com/Phileodontist/Udacity/blob/main/Data-Engineering/DL-Project/images/sparkify_data_lake_pipeline.png)

## Data
***

**Log Dataset:**
> This data set consists of log information about users' activities

```
{"artist":"Motion City Soundtrack","auth":"Logged In","firstName":"Stefany","gender":"F","itemInSession":1,"lastName":"White","length":205.26975,"level":"free","location":"Lubbock, TX","method":"PUT","page":"NextSong","registration":1540708070796.0,"sessionId":867,"song":"Fell In Love Without You (Acoustic)","status":200,"ts":1543092353796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"83"}
```


<br/>

**Song Dataset:**
> This data set consists of information about artist and their songs

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```



## Data Schema
***

With S3 as our preferred storage platform for our data lake, we ended up using it's ability to store files in a structured hierarchy to help facilitate better queries for the analytics team. By partitioning our dimension tables into partitions, based on specified columns, we were able to sort larger tables into a more managable and querable format (Parquet). This structure allows analyst to pull data based on facetable fields like year, month, or artist id to their own liking. The ability to do so would aid them in query runtimes, due to their ability to pull data specific to their query as oppose to having to do a filter through the entire dataset. 

***

**Fact Table**


   * **songPlays:** **PARTITIONED by (year, month)**
       * **songplay_id (int)  PRIMARY KEY IDENTITY(0,1)**: Unique ID for a songplay record
       * **start_time  (text)**: Start time when an event occured
       * **user_id     (int)  REFERENCES users(user_id)**: Unique ID for a user
       * **level       (text)**: Level of the account, on whether the user paid
       * **song_id     (text) REFERENCES users(song_id)**: Unique ID for a song record
       * **artist_id   (text) REFERENCES users(artist_id)**: Unique ID for an artist record
       * **session_id  (int)**: Unique ID for a session
       * **location    (text)**: Location when the event occured
       * **user_agent  (text)**: The user agent that the user used
       
***
**Dimension Tables**

   * **users**:
       * **user_id     (int)  PRIMARY KEY**: Unique ID for a user record
       * **first_name  (text)**: First name of user
       * **last_name   (text)**: last name of user
       * **gender      (text)**: Gender of the user
       * **level       (text)**: Level of the account, on whether the user paid
       
       
   * **songs**: **PARTITIONED by (year, artist_id)**
       * **song_id     (text) PRIMARY KEY**: Unique ID for a song record
       * **title       (text)**: Title of a song
       * **artist_id   (text)**: Unique ID for a artist record
       * **year        (int)**: Year that the song was released
       * **duration    (float)**: The duration of the song
       
       
   * **artist**: 
       * **artist_id   (text) PRIMARY KEY**: Unique ID for an artist record
       * **name        (text)**: Name of artist
       * **location    (text)**: Location of the artist
       * **latitude    (float)**: Latitude of the artist's location
       * **longitute   (float)**: longitute of the artist's location
       
       
   * **times**: **PARTITIONED by (year, month)**
       * **start_time  (text) PRIMARY KEY**: Start time when an event occured
       * **hour        (int)**: Hour of event occurance
       * **day         (int)**: Day of event occurance
       * **week        (int)**: Week of event occurance
       * **month       (int)**: Month of event occurance
       * **year        (int)**: Year of event occurance
       * **weekday     (int)**: Weekday of event occurance
    
***

       
## File Structure

**Input Data**
    - [S3 Endpoint]: s3a://udacity-dend/
    - [log data]: s3a://udacity-dend/log_data/
    - [song data]: s3a://udacity-dend/song_data/
    
**Output Data**
    - [S3 Endpoint]: s3a://lp-dl-project/data/
    
**ETL Pipeline**
    - [ETL Script]: etl.py - Reads data from S3, processes data via Spark, partitions data into parquet files back into S3
    - [Config File]: dl.cfg - Specifies global parameters used throughout the ETL pipeline.

  
***
