# Assignment #2
# 	Bank Marketing Campaign Data Analysis with DataFrame API
# 	a) Load Bank Marketing Dataset and create DataFrame		
# 	b) Give marketing success rate. (No. of people subscribed / total no. of entries)
# 	c) Give marketing failure rate
# 	d) Maximum, Mean, and Minimum age of the average targeted customer
# 	e) Check the quality of customers by checking the average balance, median balance of customers
# 	f) Check if age matters in marketing subscription for deposit
# 	g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.
# 	h) Check if marital status mattered for subscription to deposit.
# 	i) Check if age and marital status together mattered for subscription to deposit scheme

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("bankmarket using pyspark dataframes").getOrCreate()

# 	a) Load Bank Marketing Dataset and create DataFrame		

df= spark.read.load("/home/ram/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv",format="csv",sep=";",inferSchema=True,header=True)

# 	b) Give marketing success rate. (No. of people subscribed / total no. of entries)

total_entries=df.count()
sub_df=df.filter(df.y=="yes")

people_subscribed=sub_df.count()
success_rate=round((people_subscribed*100/total_entries),2)
print("success rate : ",success_rate)

# 	c) Give marketing failure rate
failure_rate=100-success_rate
print("failure rate",failure_rate)

# 	d) Maximum, Mean, and Minimum age of the average targeted customer

print(sub_df.select(max(col('age')),min(col('age')),round(mean(col('age')),2)).show())

# 	e) Check the quality of customers by checking the average balance, median balance of customers


print(sub_df.select(round(mean(col('balance')),2)).show())

print("median balance : ", sub_df.orderBy(col("balance").asc()).select(col('balance')).rdd.collect()[(people_subscribed//2)+1])


# 	f) Check if age matters in marketing subscription for deposit

print(sub_df.groupBy("age").agg(count('age').alias('cnt')).show(truncate=False))


# 	g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.

age_grp_df=sub_df.select(col("*"), when(col("age") <= 18,"Teenager").when(col("age") <= 40,"Youngsters").when(col("age") <= 60,"Middleagers").otherwise("Seniors").alias("age_grp"))

print( age_grp_df.groupby('age_grp').count().show())

# 	h) Check if marital status mattered for subscription to deposit.

print(sub_df.groupby('marital').count().show())

# 	i) Check if age and marital status together mattered for subscription to deposit scheme


amdf=sub_df.groupby('age','marital').count()
print(amdf.orderBy(col('count').desc()).show())






