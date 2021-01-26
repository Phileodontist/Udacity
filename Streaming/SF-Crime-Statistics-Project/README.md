# S.F. Crime Statistics Project
***

### JSON Data Converted to Binary
![alt text](https://github.com/Phileodontist/Udacity/blob/main/Streaming/SF-Crime-Statistics-Project/images/output_example_binary.png)

### Progress Reporter
![alt text](https://github.com/Phileodontist/Udacity/blob/main/Streaming/SF-Crime-Statistics-Project/images/progress_reporter.png)

### Spark-UI Dashboard
![alt text](https://github.com/Phileodontist/Udacity/blob/main/Streaming/SF-Crime-Statistics-Project/images/Spark-UI.png)


### Spark Optimization
**Queston 1:** How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
There are two parameters that were used to gauge throughput and latency, the two being: `processingTime` and `maxOffsetsPerTrigger`.

`processingTime` is used to determine the time interval in which a trigger executes the specificed query. <br/>
 Ex. Every 20 secs, a micro-batch will be produced with the results of the query.
 
 `maxOffsetsPerTrigger` is used to determine the limit in which a given trigger is able to process records. <br/>
 Ex. Per trigger, up to n records can be processed for a given batch.
 
 In adjusting `processingTime`, the higher the value is, the longer a given batch takes to process the number of records specified by the `maxOffsetsPerTrigger` parameter. This parameter increases latency, where fewer batches are processed over a period of time, hence one should adjust the number accordingly to find the right amount of time needed to process records, not exceeding the amount of time need for processing; otherwise each batch falls behind. 
 
 Adjusting `maxOffsetsPerTrigger` increases the throughput, where more records are processed for each trigger. However, this results in longer processing times per batch, depending on the number of partitions and other factors. 
 
 These two parameters can be used to optimize how spark processes a given job. Parameters such as the following: `.option("numPartitions", "n")` can decrease latency as more nodes can be used to process records quicker.


**Question 2:** What were the 2-3 most efficent SparkSession property key/value pairs? Through testing muliple variations on values, how can you tell these were the most optimal?

To further increase throughput, `spark.streaming.kafka.maxRatePerPartition` can be configured to increase the number of records retrieved from each topic partition. <br/>

To further increase latency, `spark.default.parallelism` can be configured to increase latency as more nodes can process faster in parallel.






