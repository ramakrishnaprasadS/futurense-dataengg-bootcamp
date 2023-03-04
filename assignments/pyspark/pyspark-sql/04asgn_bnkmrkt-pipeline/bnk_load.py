from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bankmarket data pipeline ---load").getOrCreate()

from_hdfspth="hdfs://localhost:9000/user/training/bankmarket/bankmarketdata.csv"
to_hdfspath="hdfs://localhost:9000/user/training/bankmarket/bnk_parquet"

#reading file to dataframe


df = spark.read.options(delimiter=";",header=True,inferSchema=True).csv(path=from_hdfspth)




# loading df to a bnk_parquet_load folder in parquet format

df.write.parquet("/mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/04asgn_bnkmrkt-pipeline/bnk_parquet_load")




