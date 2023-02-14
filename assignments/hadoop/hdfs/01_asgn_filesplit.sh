#!/bin/bash
#Create directory "weather" under /user/training
hadoop fs -mkdir /user/training/weather

#Load the data from ~/futurence_hadoop-pyspark/labs/dataset/weather to /user/training/weather
hadoop fs -put  ~/futurence_hadoop-pyspark/labs/dataset/weather /user/training/weather

#Display the weather data
hadoop fs -cat /user/training/weather/weather_data.txt

lines=wc -l ~/futurence_hadoop-pyspark/labs/dataset/weather/weather_data.txt

#Split the weather data file and store as weather1 and wearther2

awk 'NR<=500' ~/futurence_hadoop-pyspark/labs/dataset/weather/weather_data.txt > weather1.txt
awk 'NR>500' ~/futurence_hadoop-pyspark/labs/dataset/weather/weather_data.txt/weather_data.txt > weather2.txt

# Put the files back into HDFS
hdfs dfs -put ~/futurence_hadoop-pyspark/labs/dataset/weather/weather_data.txt/weather_data.txt /user/training/weather/weather1.txt
hdfs dfs -put ~/futurence_hadoop-pyspark/labs/dataset/weather/weather_data.txt/weather_data.txt /user/training/weather/weather2.txt



