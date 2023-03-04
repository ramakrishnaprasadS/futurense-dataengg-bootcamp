
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("bankmarket data pipeline ---tranform---").getOrCreate()

#reading from parquet file

df = spark.read.parquet("/mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/04asgn_bnkmrkt-pipeline/bnk_parquet_clean")


#category wise number of subscribers

df1=df.filter((df.y=='yes')).withColumn("age_grp",concat_ws(" ",lit("below"), F.ceil(df.age/5)*5,lit("years"))).groupBy('age_grp').count().orderBy('age_grp')

df1.write.mode("append") \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/pyspark_training") \
	.option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "subscribers") \
    .option("user", "pyspark") \
    .option("password", "pyspark") \
    .save()

