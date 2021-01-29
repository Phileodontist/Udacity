from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

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

spark = SparkSession.builder.appName("Redis-Stream").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

redisRawStream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers","localhost:9092") \
            .option("subscribe","redis-server") \
            .option("startingOffsets","earliest") \
            .load()

redisStream = redisRawStream.selectExpr("cast(value as string) value")

redisStream.withColumn("value", from_json("value", redisSchema)) \
            .select(col("value.*")) \
            .createOrReplaceTempView("RedisSortedSet")

redisEncodedStream = spark.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")

redisDecodedStream = redisEncodedStream.withColumn("encodedCustomer", unbase64(redisEncodedStream.encodedCustomer).cast("string"))

redisDecodedStream.withColumn("encodedCustomer", from_json("encodedCustomer", customerSchema)) \
                                .select(col("encodedCustomer.*")) \
                                .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql("select * from CustomerRecords where email is not null and birthDay is not null")

customerDF = emailAndBirthDayStreamingDF.withColumn("birthYear", split(emailAndBirthDayStreamingDF.birthDay, '-').getItem(0))
# customerDF = customerDF.select("email", "birthYear")

customerDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination() 