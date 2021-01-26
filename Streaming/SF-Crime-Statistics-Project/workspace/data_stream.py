import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

schema = StructType([
    StructField("client_id", StringType()),
    StructField("original_crime_type_name", StringType()),
    StructField("report_date", TimestampType()),
    StructField("call_date", TimestampType()),
    StructField("offense_date", TimestampType()),
    StructField("call_time", StringType()),
    StructField("call_date_time", TimestampType()),
    StructField("disposition", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("agency_id", StringType()),
    StructField("address_type", StringType()),
    StructField("common_location", StringType())
])

def run_spark_job(spark):
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police-calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 400) \
        .option("stopGracefullyOnShutDown", "true") \
        .load()
    
    # Show schema for the incoming resources for checks
    df.printSchema()

    # Retrieve value field
    kafka_df = df.selectExpr("CAST(value as STRING)")

    # Retrieve all fields from value field
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    # count the number of original crime type, use 60 min latethreshold
    agg_df = service_table \
           .select(["original_crime_type_name", "disposition", "call_date_time"]) \
           .withWatermark("call_date_time", "60 minutes") \
           .groupby(["original_crime_type_name", "disposition"]) \
           .count()

    # Batch every n secs
    #count_query = agg_df \
    #    .writeStream \
    #    .outputMode("Update") \
    #    .trigger(processingTime="20 seconds") \
    #    .format("console") \
    #    .start()
    
    agg_df = service_table \
            .select(["original_crime_type_name", "disposition", "call_date_time"]) \
            .withWatermark("call_date_time", "60 minutes") \
            .groupby(psf.window("call_date_time", "10 minutes", "5 minutes"), "original_crime_type_name", "disposition") \
            .count()        

    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = agg_df.join(radio_code_df, "disposition")
    join_query = join_query.select("window", "original_crime_type_name", "description", "count")
    
    # Batch every n secs
    join_query = join_query \
        .writeStream \
        .outputMode("Complete") \
        .trigger(processingTime="17 seconds") \
        .format("console") \
        .option("truncate", "false") \
        .start() \

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", "3000") \
        .config("spark.streaming.kafka.maxRatePerPartition", "3") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
