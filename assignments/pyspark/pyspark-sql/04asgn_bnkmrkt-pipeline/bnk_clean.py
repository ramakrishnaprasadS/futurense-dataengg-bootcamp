
from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bankmarket data pipeline ---clean").getOrCreate()

#reading from bnk_parquet_load

df = spark.read.parquet("/mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/04asgn_bnkmrkt-pipeline/bnk_parquet_load")

# age validation
df1=df.filter(df.age.isNotNull())

# writing df to bnk_parquet_clean folder in parquet format

df1.write.parquet("/mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/04asgn_bnkmrkt-pipeline/bnk_parquet_clean")







