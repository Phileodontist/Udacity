# Optimizing Public Transportation
***

## Synopsis
The Chicago Transit Authority (CTA) has asked us to develop a dashboard displaying system status for its commuters. 
<br>```Note: This is a hypothetical request; seed data was retrieved from the CTA's website and used to generate additional data for this project.```

In response to this request, a streaming event pipeline was developed to ingest, process and aggregate stream data to fuel the statistics on the proposed dashboard.

![](https://video.udacity-data.com/topher/2019/July/5d320154_screen-shot-2019-07-19-at-10.43.38-am/screen-shot-2019-07-19-at-10.43.38-am.png)

The three main data sources consist of a **database**, **arrival and turnstiles sensors**, and lastly **weather sensors**. Data retrieved from these three sources are consumed via Kafka and processed via KSQL.

## Datasets
The data in [this directory](https://github.com/Phileodontist/Udacity/tree/main/Streaming/Optimizing-Public-Transporation-Project/workspace/producers/data) is used as the seed data that simulator uses to generate event data.

Much of the data in the directory is derived from the (City of Chicago Transit Authority's open
datasets](https://www.transitchicago.com/data/). The following datasets originate from this source:

* [`cta_stations.csv`](cta_stations.csv)
* [`ridership_seed.csv`](ridership_seed.csv)

The data from these sources has been truncated and some columns were added to make the simulation
more useful.

## Dashboard Preview
![](https://video.udacity-data.com/topher/2019/July/5d352372_screen-shot-2019-07-19-at-10.41.29-am/screen-shot-2019-07-19-at-10.41.29-am.png)
