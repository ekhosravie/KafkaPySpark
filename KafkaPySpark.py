from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkStructuredStreaming") \
    .config("spark.jars", "/path/to/kafka-jars/*") \
    .getOrCreate()

schema = StructType([
    StructField("name", StringType()),
    StructField("age", StringType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .selectExpr("data.name", "data.age")

df.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()