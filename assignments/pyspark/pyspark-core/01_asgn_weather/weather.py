from pyspark import SparkContext

sc = SparkContext("local", "weather app")

lines = sc.textFile("/home/ram/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt")

lines1=lines.map(lambda x: x.split())
lines2=lines1.map(lambda x:("max_temp",float(x[5])))
linesmax=lines2.reduceByKey(lambda x,y:max(x,y))

l1=lines.map(lambda x: x.split())
l2=l1.map(lambda x:("min_temp",float(x[6])))
lmin=l2.reduceByKey(lambda x,y:min(x,y))

for each in lines.flatMap(lambda x: [[x.split()[1]]+[x.split()[5]]]).map(lambda x: (x[0][4:6],float(x[1]))).reduceByKey(lambda a,b:min(a,b)).collect():
    print(each[0],"min_temp",each[1])
for each in lines.flatMap(lambda x: [[x.split()[1]]+[x.split()[6]]]).map(lambda x: (x[0][4:6],float(x[1]))).reduceByKey(lambda a,b:max(a,b)).collect():
    print(each[0],"max_temp",each[1])

print(lmin.collect())
print(linesmax.collect())




