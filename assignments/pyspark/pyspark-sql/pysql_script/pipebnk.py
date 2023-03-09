from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bankmarket data pipeline").getOrCreate()


#reading file to dataframe
df = spark.read.options(delimiter=";",header=True).csv("/home/ram/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv")

#creating temp table from dataframe
df.createOrReplaceTempView("bankmarketing")

#performing the required query
qresult=spark.sql("select case when age<13 then 'kids' when age>=13 and age<=19 then 'teenagers' when age>=20 and age<=30 then 'youngsters' when age>=31 and age<=50 then 'middle-agers' when age>50 then 'seniors' else 'NA' end as tpe ,count(*) as no_of_subscribers from bankmarketing where y='yes' group by tpe order by no_of_subscribers desc")

#writing the query result to a parquet format
qresult.write.parquet("/mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/bnk_parquet")

#reading data from parquet file to dataframe
df1=spark.read.parquet("/mnt/c/Users/miles/Documents/Github/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql/bnk_parquet")

#displaying the data
print(df1.show())



