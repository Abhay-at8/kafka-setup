import findspark
findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
# os.environ['HADOOP_HOME'] = 'C:\\Users\\priya\\hadoop'
# Create Spark session with Kafka support
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
# Include Kafka package
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
kafkahost=os.environ.get("KAFKA_HOST", "localhost")
# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaSparkOrdersAnalytics") \
    .getOrCreate()
kafka_broker = kafkahost+":9092"

# Kafka input config
kafka_input_config = {
    "kafka.bootstrap.servers": kafka_broker,
    "subscribe": "orders",
    "startOffsets" : "latest",
    "failOnDataLoss" : "false"
}

kafka_output_config = {
    "kafka.bootstrap.servers": kafka_broker,
    "topic" : "output",
    "checkpointLocation" : "/home/cloudlab/check.txt"
}

# Inptu Schema
orderline_item_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("medicineName", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unitPrice", DoubleType(), True),
    StructField("totalPrice", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("orderId", IntegerType(), True)
])

order_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("patientId", IntegerType(), True),
    StructField("doctorId", IntegerType(), True),
    StructField("prescriptionId", IntegerType(), True),
    StructField("doDelivery", StringType(), True),
    StructField("deliveryCharges", DoubleType(), True),
    StructField("totalCharges", DoubleType(), True),
    StructField("orderlineItems", ArrayType(orderline_item_schema), True)
])
# Kafka topic and broker details
# kafka_topic = "orders"


# Read data from Kafka topic as a streaming DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", "orders") \
    .option("startOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), order_schema).alias("json_data")) \
    .select("json_data.*")

# Perform simple analytics, e.g., calculate total revenue by medicineName
order_items_df = df.withColumn("orderlineItem", F.col("orderlineItems")) \
    .selectExpr("id", "explode(orderlineItem) as item") \
    .select(
        F.col("id"),
        F.col("item.medicineName"),
        F.col("item.quantity"),
        F.col("item.unitPrice"),
        F.col("item.totalPrice"),
        F.col("item.status")
    )

# Check output of readstream
revenue_by_medicine = order_items_df.groupBy("medicineName") \
    .sum("totalPrice") \
    .withColumnRenamed("sum(totalPrice)", "totalRevenue")

# Convert to JSON string format before writing to Kafka
# output_df = revenue_by_medicine.select(
#     F.to_json(F.struct("medicineName", "totalRevenue")).alias("value")
# )

# output_df = df.select(F.to_json(F.struct(*df.columns)).alias("value"))

write = revenue_by_medicine \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

write.awaitTermination()