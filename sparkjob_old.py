import findspark
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

findspark.init()

import os
# Include Kafka package
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
kafkahost=os.environ.get("KAFKA_HOST", "localhost")

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaSparkOrdersAnalytics") \
    .getOrCreate()
kafka_broker = kafkahost + ":9092"

# Kafka input config
kafka_input_config = {
    "kafka.bootstrap.servers": kafka_broker,
    "subscribe": "orders",
    "startOffsets": "latest",
    "failOnDataLoss": "false"
}

# Input Schema
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

# Function to retry connection
def retry_connection(spark, kafka_input_config, retries=3, delay=60):
    for attempt in range(retries):
        try:
            # Read data from Kafka topic as a streaming DataFrame
            df = spark.readStream \
                .format("kafka") \
                .options(**kafka_input_config) \
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

            # Calculate total revenue by medicine name
            revenue_by_medicine = order_items_df.groupBy("medicineName") \
                .sum("totalPrice") \
                .withColumnRenamed("sum(totalPrice)", "totalRevenue")

            # Write the stream to the console
            write = revenue_by_medicine \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .start()

            write.awaitTermination()
            break

        except Exception as e:
            print(f"Attempt {attempt + 1} failed with exception: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("All attempts failed. Exiting.")
                raise

# Retry connection for 3 minutes (3 retries with a delay of 1 minute each)
retry_connection(spark, kafka_input_config, retries=3, delay=60)
