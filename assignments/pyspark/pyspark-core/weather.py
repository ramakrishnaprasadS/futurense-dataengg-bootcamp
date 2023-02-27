from pyspark import SparkContext

sc = SparkContext("local", "ratings app")

lines = sc.textFile("/home/ram/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt")

lines1=lines.map(lambda x: x.split())
lines2=lines1.map(lambda x:("max_temp",float(x[5])))
lines3=lines2.reduceByKey(lambda x,y:max(x,y))

l1=lines.map(lambda x: x.split())
l2=l1.map(lambda x:("min_temp",float(x[5])))
l3=l2.reduceByKey(lambda x,y:min(x,y))

print(l3.collect())
print(lines3.collect())
