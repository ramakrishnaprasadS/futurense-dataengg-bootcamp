#Assignment #1

#	Bank Marketing Campaign Data Analysis with PySpark Structured Streaming

#	Create PySpark Application - bank-marketing-stream-analysis.py. Perform below operations.

#a) Consume Bank Marketing Campaign event from Kafka topic - bank-marketing-events

        #Consume Message (from beginning)

#pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1
#create kafka topic
##  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic bank-marketing-events


#Produce Message
#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic bank-marketing-events



from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructField, StructType
from pyspark.sql.functions import *

#Create Spark Session
spark = SparkSession \
    .builder \
    .appName("01asgn_bnk-market-kafka-StreamingAnalysis") \
    .getOrCreate()

# Path to our loan JSON files
#inputPath = "/home/ubuntu/futurense_hadoop-pyspark/labs/dataset/bankmarket"

#Consume message from Kafka topic and create Dataset


df = spark.readStream.format("kafka")\
      .option("kafka.bootstrap.servers", "localhost:9092")\
      .option("subscribe", "bank-marketing-events")\
	  .option("startingOffsets", "earliest")\
	  .load()

schema = StructType([ StructField("col1", StringType(), True),
                      StructField("col2", StringType(), True),
                      StructField("col3", StringType(), True),
                      StructField("col4", StringType(), True),
                      StructField("col5", StringType(), True),
                      StructField("col6", StringType(), True),
                      StructField("col7", StringType(), True),
                      StructField("col8", StringType(), True),
                      StructField("col9", StringType(), True),
                      StructField("col10", StringType(), True),
                      StructField("col11", StringType(), True),
                      StructField("col12", StringType(), True),
                      StructField("col13", StringType(), True),
                      StructField("col14", StringType(), True),
                      StructField("col15", StringType(), True),
                      StructField("col16", StringType(), True),
                      StructField("col17", StringType(), True)
                      ])


#Converting received kafka binary message to json message applying custom schema
#convdf = df.select(from_csv(col("value").cast("String"),schema).alias("record1"))

 df1=df.select(col('value').cast("string"))

query = df1\
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "sm") \
    .start()






