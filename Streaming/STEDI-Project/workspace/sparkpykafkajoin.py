from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

redisSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

stediSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

spark = SparkSession.builder.appName("STEDI Dashboard").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

redisRawStream = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "redis-server") \
                .option("startingOffsets", "earliest") \
                .load()

redisDF = redisRawStream.selectExpr("cast(value as string) value")

redisStreamingDF = redisDF.withColumn("value", from_json("value", redisSchema)) \
                .select(col("value.*")) \
                .createOrReplaceTempView("RedisSortedSet")

redisEncodedDF = spark.sql("select key, zSetEntries[0].element as redisEvent from RedisSortedSet")

redisDecodedDF = redisEncodedDF.withColumn("redisEvent", unbase64(redisEncodedDF.redisEvent).cast("string"))

redisDecodedDF.withColumn("customer", from_json("redisEvent", customerSchema)) \
                            .select(col("customer.*")) \
                            .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql("select * from CustomerRecords where email is not null and birthDay is not null")

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.withColumn("birthYear", split(emailAndBirthDayStreamingDF['birthDay'], '-').getItem(0))

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

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

customerProfile = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, expr("customer = email"))
customerProfile = customerProfile.select("customer", "score", "email", "birthYear")

# Write to console
# customerProfile.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()

customerProfile.selectExpr("cast(email as string) key", "to_json(struct(*)) value") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "stedi-graph") \
  .option("checkpointLocation","/tmp/kafkacheckpoint2")\
  .start() \
  .awaitTermination()