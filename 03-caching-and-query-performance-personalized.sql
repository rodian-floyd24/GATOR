-- Caching and Query Performance (personalized, idempotent)
-- Uses: ROLE TRAINING_ROLE, WH GATOR_WH, DB GATOR_DB.PUBLIC

-- 3.1.0 Setup context and a writable schema
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS GATOR_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE GATOR_WH;

-- If you lack privilege to create DBs, comment the next line and just USE
CREATE DATABASE IF NOT EXISTS GATOR_DB;
USE SCHEMA GATOR_DB.PUBLIC;

-- Clear data cache on the warehouse
ALTER WAREHOUSE GATOR_WH RESUME;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;

-- 3.1.4 Create a local table from lab data
CREATE OR REPLACE TABLE CUSTOMER AS
SELECT c_custkey, c_firstname, c_lastname
FROM SNOWBEARAIR_DB.PROMO_CATALOG_SALES.CUSTOMER;

ALTER WAREHOUSE GATOR_WH RESUME;
ALTER WAREHOUSE GATOR_WH SUSPEND;

-- 3.1.5 Simple query to inspect in Query Profile
SELECT DISTINCT * FROM CUSTOMER;

-- 3.2.0 Metadata cache demo (disable result cache)
USE ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF10;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT MIN(ps_partkey), MAX(ps_partkey) FROM PARTSUPP;

-- 3.3.0 Data cache demo (disable result cache and reset warehouse)
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE GATOR_WH RESUME;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;

-- First run (expect 0% from cache)
SELECT ps_partkey, ps_availqty
FROM PARTSUPP
WHERE ps_partkey > 1000000;

-- Second run (expect ~100% from cache)
SELECT ps_partkey, ps_availqty
FROM PARTSUPP
WHERE ps_partkey > 1000000;

-- Add more columns (expect lower cache %)
SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment
FROM PARTSUPP
WHERE ps_partkey > 1000000;

-- 3.4.0 Partition pruning demo on larger dataset
USE ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE GATOR_WH RESUME;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;

-- With pruning (filters include clustering key)
SELECT  
  C_CUSTOMER_SK,
  C_LAST_NAME,
  (CA_STREET_NUMBER || ' ' || CA_STREET_NAME) AS CUST_ADDRESS,
  CA_CITY,
  CA_STATE
FROM CUSTOMER
JOIN CUSTOMER_ADDRESS ON C_CUSTOMER_ID = CA_ADDRESS_ID
WHERE C_CUSTOMER_SK BETWEEN 100000 AND 600000
  AND C_LAST_NAME LIKE 'Johnson'
ORDER BY CA_CITY, CA_STATE;

-- Without pruning (omit clustering key)
SELECT  
  C_CUSTOMER_SK,
  C_LAST_NAME,
  (CA_STREET_NUMBER || ' ' || CA_STREET_NAME) AS CUST_ADDRESS,
  CA_CITY,
  CA_STATE
FROM CUSTOMER
JOIN CUSTOMER_ADDRESS ON C_CUSTOMER_ID = CA_ADDRESS_ID
WHERE C_LAST_NAME = 'Johnson'
ORDER BY CA_CITY, CA_STATE;

-- 3.5.0 Spillage investigation
USE ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';

ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE GATOR_WH RESUME;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;

-- Query that may spill (nested aggregation)
SELECT 
  cd_gender,
  AVG(lp) AS average_list_price,
  AVG(sp) AS average_sales_price,
  AVG(qu) AS average_quantity
FROM (
  SELECT 
    cd_gender,
    cs_order_number,
    AVG(cs_list_price) AS lp,
    AVG(cs_sales_price) AS sp,
    AVG(cs_quantity) AS qu
  FROM catalog_sales, date_dim, customer_demographics
  WHERE cs_sold_date_sk = d_date_sk
    AND cs_bill_cdemo_sk = cd_demo_sk
    AND d_year = 2000
    AND d_moy IN (1,2,3,4,5,6,7,8,9,10)
  GROUP BY cd_gender, cs_order_number
) inner_query
GROUP BY cd_gender;

-- Optimized version (no outer aggregation needed)
ALTER WAREHOUSE GATOR_WH RESUME;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;

SELECT 
  cd_gender,
  AVG(cs_list_price) AS lp,
  AVG(cs_sales_price) AS sp,
  AVG(cs_quantity) AS qu
FROM catalog_sales, date_dim, customer_demographics
WHERE cs_sold_date_sk = d_date_sk
  AND cs_bill_cdemo_sk = cd_demo_sk
  AND d_year = 2000
  AND d_moy IN (1,2,3,4,5,6,7,8,9,10)
GROUP BY cd_gender;

-- 3.6.0 EXPLAIN plans
EXPLAIN
SELECT 
  cd_gender,
  AVG(lp) AS average_list_price,
  AVG(sp) AS average_sales_price,
  AVG(qu) AS average_quantity
FROM (
  SELECT 
    cd_gender,
    cs_order_number,
    AVG(cs_list_price) AS lp,
    AVG(cs_sales_price) AS sp,
    AVG(cs_quantity) AS qu
  FROM catalog_sales, date_dim, customer_demographics
  WHERE cs_sold_date_sk = d_date_sk
    AND cs_bill_cdemo_sk = cd_demo_sk
    AND d_year = 2000
    AND d_moy IN (1,2,3,4,5,6,7,8,9,10)
  GROUP BY cd_gender, cs_order_number
) inner_query
GROUP BY cd_gender;

EXPLAIN
SELECT 
  cd_gender,
  AVG(cs_list_price) AS lp,
  AVG(cs_sales_price) AS sp,
  AVG(cs_quantity) AS qu
FROM catalog_sales, date_dim, customer_demographics
WHERE cs_sold_date_sk = d_date_sk
  AND cs_bill_cdemo_sk = cd_demo_sk
  AND d_year = 2000
  AND d_moy IN (1,2,3,4,5,6,7,8,9,10)
GROUP BY cd_gender;

-- Reset size and suspend
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER WAREHOUSE GATOR_WH RESUME;
ALTER WAREHOUSE GATOR_WH SUSPEND;
