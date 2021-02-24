CREATE DATABASE IF NOT EXISTS factory;
USE factory;

DROP TABLE IF EXISTS experimental_motor_enriched;
CREATE EXTERNAL TABLE experimental_motor_enriched
  (  serial_no         STRING
   , vin               STRING
   , model             STRING
   , zip               INTEGER
   , customer_id       INTEGER
   , username          STRING
   , name              STRING
   , gender            CHAR(1)
   , email             STRING
   , occupation        STRING
   , birthdate         DATE
   , address           STRING
   , salary            FLOAT
   , sale_date         DATE
   , saleprice         FLOAT
   , latitude          STRING
   , longitude         STRING
   , factory_no        INTEGER
   , machine_no        INTEGER
   , part_no           STRING
   , local_timestamp   DOUBLE
   , status            STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (  'separatorChar' = ','
                      , 'quoteChar'  = '"')
-- IMPORTANT: Update S3BUCKET with S3 bucket location
-- LOCATION '<S3BUCKET>'
LOCATION 's3a://usermarketing-cdp-demo/gdeleon/report/'
tblproperties('skip.header.line.count' = '1');


-- ERROR IN PARTS FOUND BETWEEN October 22 and October 24 (26 parts)
select sale_date, vin, name, address, latitude, longitude FROM experimental_motor_enriched where sale_date between CAST('2020-10-22' AS DATE) AND CAST('2020-10-24' AS DATE) order by sale_date  desc;



--- TEST PURPOSE ---
-- 2020-10-24	4
-- 2020-10-23	15
-- 2020-10-22	7
select sale_date, count(sale_date) FROM experimental_motor_enriched where sale_date between CAST('2020-10-22' AS DATE) AND CAST('2020-10-24' AS DATE) group by sale_date order by 1 desc;

select sale_date, count(sale_date) FROM experimental_motor_enriched group by sale_date order by 2 desc;

select part_no, count(part_no) FROM experimental_motor_enriched group by part_no order by 2 desc;

select factory_no, count(factory_no) FROM experimental_motor_enriched group by factory_no order by 2 desc;

select * FROM experimental_motor_enriched LIMIT 10;

describe formatted experimental_motor_enriched;




