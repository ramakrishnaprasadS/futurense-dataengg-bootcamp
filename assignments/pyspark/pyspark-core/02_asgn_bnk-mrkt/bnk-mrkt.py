# Assignment #2
# 	Bank Marketing Campaign Data Analysis with RDD API
# 	a) Load Bank Marketing Dataset and create RDD		
# 	b) Give marketing success rate. (No. of people subscribed / total no. of entries)
# 	c) Give marketing failure rate
# 	d) Maximum, Mean, and Minimum age of the average targeted customer
# 	e) Check the quality of customers by checking the average balance, median balance of customers
# 	f) Check if age matters in marketing subscription for deposit
# 	g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.
# 	h) Check if marital status mattered for subscription to deposit.
# 	i) Check if age and marital status together mattered for subscription to deposit scheme

from pyspark import SparkContext 

sc = SparkContext("local", "Weather app")

# 	a) Load Bank Marketing Dataset and create RDD	
bnk=sc.textFile("/home/ram/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv")

# 	b) Give marketing success rate. (No. of people subscribed / total no. of entries)
bank_data = bnk.collect()
line1 = bank_data[0]
data = bank_data[1:]

rdd = sc.parallelize(data)

subscribers_count = rdd.filter(lambda x: 'yes' in  x.split(';')[-1]).count()
all_entries = rdd.count()

success_rate = subscribers_count / all_entries * 100

print("Success Rate : ", round(success_rate,2))


# 	c) Give marketing failure rate

failure_rate = 100 - success_rate

print("Failure Rate : ", round(failure_rate))


# 	d) Maximum, Mean, and Minimum age of the average targeted customer

min_age=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:int(x.split(";")[0])).min()
max_age=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:int(x.split(";")[0])).max()
mean_age=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:int(x.split(";")[0])).mean()

print("mean age : ",round(mean_age,2))
print("minimum age : ",round(min_age,2))
print("maximum age : ",round(max_age,2))

# 	e) Check the quality of customers by checking the average balance, median balance of customers


avg_balance=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:float(x.split(";")[5])).mean()
print("average balance",round(avg_balance,2))

median_balance=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:float(x.split(";")[5])).sortBy(lambda x: x).collect()[(subscribers_count//2)+1]

print("median balance",median_balance)

# 	f) Check if age matters in marketing subscription for deposit

age_wise_subcnt=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:(int(x.split(";")[0]),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0]).collect()

print("age_wise_subcnt",age_wise_subcnt)

# 	g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.

def agegrp(x):
    age=int(x.split(";")[0])
    if age<=18:
        return "Teenager"
    elif age<=40:
        return "Youngster"
    elif age<=60:
        return "MiddleAger"
    else:
        return "Seniors"

agegrp_wise_subcnt=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:(agegrp(x),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0]).collect()

print("age group wise subscibers count",agegrp_wise_subcnt)


# 	h) Check if marital status mattered for subscription to deposit.

marital_status_subcnt=rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:(x.split(";")[2],1)).reduceByKey(lambda x,y:x+y).collect()

print("marital status-subscriber count",marital_status_subcnt)

# 	i) Check if age and marital status together mattered for subscription to deposit scheme
for key, val in rdd.filter(lambda x:'yes' in x.split(";")[-1]).map(lambda x:(x.split(";")[0],x.split(";")[2])).groupBy(lambda x:x).map(lambda x:(x[0],len(x[1]))).sortBy(lambda x:x[1],ascending=False).collect():
    print(key,"-->",val)

