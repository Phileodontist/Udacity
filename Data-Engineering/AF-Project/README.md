# Sparkify Airflow Data Pipeline 

## Synopsis
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines 
and come to the conclusion that the best tool to achieve this is Apache Airflow. As a result, a Airflow pipeline is needed to orchestrate
the execution of a series of steps to ingest, process and validate data retrieved from their ETL pipelines.

The following pipeline provides an automated way of updating the data warehouse with a validation step. This allows flexibility in terms of manually
running the pipeline or scheduling it to run at whatever frequency.

## Airflow Pipeline Overview
![](https://video.udacity-data.com/topher/2019/January/5c48a861_example-dag/example-dag.png)

**As seen above, the series of steps consist of the following:**
1) Pull song and events data from S3
2) Load data into AWS Redshift
3) Transform data into the data warehouse model (fact table and dimension tables)
4) Validate data

## Datasets
**Events Dataset:**
>This data set consists of information about events done by users

**Songs Datasets**
>This data set consists of information about artist and their songs
