//Creating HIVE table from the data on HDFS
CREATE TABLE IF NOT EXISTS NYC_TAXI_DATA ( 
vendorId INT, 
pickup_time timestamp,
dropoff_time timestamp,
passanger_count INT,
trip_distancce decimal(10,5),
pick_lng decimal(14,10),
pick_lat decimal(14,10),
ratecodeid INT,
store_flg String,
drop_lng decimal(14,10),
drop_lat decimal(14,10),
pay_type INT,
fare_amount decimal(6,2),
extra decimal(6,2),
mta_tax decimal(6,2),
tip_amount decimal(6,2),
toll_amount decimal(6,2),
total_amount decimal(6,2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
LOCATION "/NYC_Taxi_Data/";

//frequent pickup location
select round(pick_lat,3),round(pick_lng,3),count(*) as count 
from NYC_TAXI_DATA 
group by round(pick_lat,3),round(pick_lng,3) 
order by count;

//frequent drop off location
select round(drop_lat,3),round(drop_lng,3),count(*) as count from NYC_TAXI_DATA group by round(drop_lat,3),round(drop_lng,3) order by count;

//frequent trip
select round(pick_lat,2),round(pick_lng,2),round(drop_lat,2),round(drop_lng,2),count(*) as count from NYC_TAXI_DATA group by round(pick_lat,2),round(pick_lng,2),round(drop_lat,2),round(drop_lng,2) order by count;