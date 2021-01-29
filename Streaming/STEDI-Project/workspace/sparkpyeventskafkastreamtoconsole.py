from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

stediSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)
spark = SparkSession.builder.appName("Customer-Risk").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

stediRawStream = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "stedi-events") \
                .option("startingOffsets", "earliest") \
                .load()
                                   
stediStream = stediRawStream.selectExpr("cast(value as string) value")

stediStream.withColumn("value", from_json("value", stediSchema)) \
                                .select(col("value.*")) \
                                .createOrReplaceTempView("CustomerRisk")

riskDF = spark.sql("select customer, score from CustomerRisk")

riskDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination() 