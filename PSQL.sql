-- search
CREATE TABLE search_tb (
userid int,
city varchar(20),
transaction_id varchar(100) PRIMARY KEY,
geocoordinates varchar(100),
time_stamp timestamp
);

-- change the datestyle
SET DATESTYLE = 'ISO, DMY';

-- import data from csv to search table
copy search_tb from 'E:\Job Tasks\NinijaCart\search.csv' delimiter ',' csv header;

-- select
CREATE TABLE select_tb (
	transaction_id varchar(100) PRIMARY KEY,
	FOREIGN KEY (transaction_id) REFERENCES search_tb(transaction_id)
);

-- import data from csv to customer table
copy select_tb from 'E:\Job Tasks\NinijaCart\select.csv' delimiter ',' csv header;

-- confirm
CREATE TABLE confirm_tb (
	transaction_id varchar(100) PRIMARY KEY,
	FOREIGN KEY (transaction_id) REFERENCES search_tb(transaction_id)
);

-- import data from csv to customer table
copy confirm_tb from 'E:\Job Tasks\NinijaCart\confirm.csv' delimiter ',' csv header;

-- success
CREATE TABLE success_tb (
	transaction_id varchar(100) PRIMARY KEY,
	FOREIGN KEY (transaction_id) REFERENCES search_tb(transaction_id)
);

-- import data from csv to customer table
copy success_tb from 'E:\Job Tasks\NinijaCart\success.csv' delimiter ',' csv header;

SELECT * FROM search_tb;
SELECT * FROM select_tb;
SELECT * FROM confirm_tb;
SELECT * FROM success_tb;

-- QUERIES
-- Q2: Write a sql query to rank the searches by the user based on timestamp
SELECT userid, transaction_id, time_stamp,
DENSE_RANK() OVER(PARTITION BY userid ORDER BY time_stamp) AS rnk
FROM search_tb;


-- Q: What hour did the users first searched an item?
WITH atb AS (
SELECT userid, transaction_id, EXTRACT(HOUR FROM time_stamp) AS time_stamp,
DENSE_RANK() OVER(PARTITION BY userid ORDER BY time_stamp) AS rnk
FROM search_tb)

SELECT time_stamp, COUNT(time_stamp) search_count
FROM atb
WHERE rnk=1
GROUP BY time_stamp
ORDER BY search_count DESC
LIMIT 1;


-- Q: What is the peak purchase hour?
WITH btb AS (
SELECT sus.transaction_id, EXTRACT(HOUR FROM s.time_stamp) AS time_stamp,
DENSE_RANK() OVER(PARTITION BY userid ORDER BY s.time_stamp) AS rnk
FROM search_tb AS s
RIGHT JOIN
success_tb sus ON sus.transaction_id = s.transaction_id)

SELECT time_stamp, COUNT(time_stamp) AS order_count
FROM btb
WHERE rnk=1
GROUP BY time_stamp
ORDER BY order_count DESC
LIMIT 1;