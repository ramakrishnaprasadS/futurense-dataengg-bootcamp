
CREATE TABLE IF NOT EXISTS customers
(
cust_id	 int,
last_name String,
first_name String,
age  int,
profession String
)
COMMENT 'Customer Details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';



LOAD DATA LOCAL INPATH '/home/ram/futurense_hadoop-pyspark/labs/dataset/retail/customers.txt' OVERWRITE INTO TABLE customers;


CREATE TABLE IF NOT EXISTS transactions 
(
trans_id  int,
trans_date  date,
cust_id  int,
amount  double,
category  String,
desc  String,
city  String,
state  String,
pymt_mode  String
)
COMMENT 'Transaction Details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;



LOAD DATA LOCAL INPATH '/mnt/c/Users/miles/Documents/GitHub/futurense-dataengg-bootcamp/assignments/hive/transactions_fine.txt' OVERWRITE INTO TABLE transactions;





Hive queries:

--1) No of transactions by customer

select c.cust_id,count(t.trans_id) as no_of_transactions from 
customers c inner join transactions t using(cust_id)
group by c.cust_id;


--2) Total transaction amount by customer

select c.cust_id,sum(t.amount) as total_transac_amt from 
customers c inner join transactions t using(cust_id)
group by c.cust_id;

--3) Get top 3 customers by transaction amount

select c.cust_id,sum(t.amount) as total_transac_amt from 
customers c inner join transactions t using(cust_id)
group by c.cust_id
order by total_transac_amt desc
limit 3;

--4) No of transactions by customer and mode of payment

select c.cust_id,t.pymt_mode,count(t.trans_id) as no_of_transactions from 
customers c inner join transactions t using(cust_id)
group by c.cust_id,t.pymt_mode;


--5) Get top 3 cities which has more transactions
select city,no_of_transactions from
    (select t.city,count(t.trans_id) as no_of_transactions,
    dense_rank() over(order by count(t.trans_id) desc) as drnk
    from 
    customers c inner join transactions t using(cust_id)
    group by t.city) D
where drnk<=3;

--6) Get month wise highest transaction

select month(t.trans_date) as month_num,max(t.amount) as max_transac_amt from 
customers c inner join transactions t using(cust_id)
group by month(t.trans_date);

--7) Get sample transactions

select * from transactions limit 10;





