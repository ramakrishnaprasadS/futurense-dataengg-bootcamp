
----1.Load data and create a Hive table

CREATE EXTERNAL TABLE IF NOT EXISTS bankmarket
(age int,job String,marital String,education String,default String,balance int,housing String,loan String,contact String,day int,month String,duration int,campaign int,pdays int,previous int,poutcome String,y String)
COMMENT 'bankmarket details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
LOCATION '/user/training/casestudy/bankmarketdata/';




---2.Give marketing success rate. (No. of people subscribed / total no. of entries)

select sum(if(y="yes",1,0)) as no_of_subscribers,
count(*) as total_entries,
round((sum(if(y="yes",1,0))/count(*))*100,2) as success_rate
from bankmarket;


---3.Give marketing failure rate

select sum(if(y="no",1,0)) as not_subscribed_count,
count(*) as total_entries,
round((sum(if(y="no",1,0))/count(*))*100,2) as failure_rate
from bankmarket;


---4.Maximum, Mean, and Minimum age of the average targeted customer

select max(age) as max_age_targeted,
round(avg(age)) as mean_age_targeted,
min(age) as min_age_targeted
from bankmarket;


---5.Check the quality of customers by checking the average balance, median balance of customers

select round(avg(balance)) as avg_balance,
percentile(balance,0.5) as median_balance,
max(balance) as max_balance
from bankmarket;

---6.Check if age matters in marketing subscription for deposit

select min(age) as min_age,
percentile(age,0.5) as median_age,
avg(age) as mean_age,
max(age) as max_age
from bankmarket
where y="yes";

select age,count(*) as no_of_subscribers
from bankmarket
where y="yes"
group by age
order by no_of_subscribers desc;


---7.Check if marital status mattered for subscription to deposit.

select sum(if(marital="single",1,0)) as no_of_unmarried_subscribers,
sum(if(marital="married",1,0)) as no_of_married_subscribers
from bankmarket
where y="yes";

select sum(if(marital="single",1,0)) as not_subscribed_unmarried,
sum(if(marital="married",1,0)) as not_subscribed_married
from bankmarket
where y="no";


---8.Check if age and marital status together mattered for subscription to deposit scheme

select age,marital,count(*) as no_of_subscibers
from bankmarket
where y="yes"
group by age,marital
order by no_of_subscibers desc
limit 50;


select age,marital,count(*) as not_subscibed_count
from bankmarket
where y="no"
group by age,marital
order by not_subscibed_count desc
limit 50;