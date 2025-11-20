-- Data Transformation During Data Loading (personalized, idempotent)
-- Uses: ROLE TRAINING_ROLE, WH GATOR_WH, DB GATOR_DB.GATOR_TRANSFORM
-- External data: TRAINING_DB.TRAININGLAB.ED_STAGE (instructor-provided)

-- 5.1.0 Setup context (create/use WH, DB, SCHEMA)
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS GATOR_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE GATOR_WH;

CREATE DATABASE IF NOT EXISTS GATOR_DB;
CREATE SCHEMA IF NOT EXISTS GATOR_DB.GATOR_TRANSFORM;
USE SCHEMA GATOR_DB.GATOR_TRANSFORM;

-- 5.1.4 Discover candidate files on the external stage
LIST @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/ PATTERN='.*\\.tbl.*';

-- 5.1.5 Quick peek at columns (no file format yet)
SELECT n.$1, n.$2, n.$3
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl n;

-- 5.1.6 Define a pipe-delimited file format in current schema
CREATE OR REPLACE FILE FORMAT MYPIPEFORMAT
  TYPE = CSV
  COMPRESSION = NONE
  FIELD_DELIMITER = '|'
  FILE_EXTENSION = 'tbl'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- 5.1.7 Create target table (with derived REGION, REGION_CODE)
CREATE OR REPLACE TABLE NATION (
  NATION_KEY  INTEGER,
  REGION      VARCHAR,
  REGION_CODE VARCHAR,
  NATION      VARCHAR,
  COMMENTS    VARCHAR
);

-- 5.1.8 Preview parsed columns using the file format
SELECT 
  n.$1 AS N_KEY,
  n.$2 AS N_NAME,
  n.$3 AS R_KEY,
  n.$4 AS N_COMMENT
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n;

-- 5.1.9 Shape and transform data to match target table
SELECT    
  n.$1 AS N_KEY,
  CASE 
    WHEN n.$3 = 0 THEN 'AFRICA' 
    WHEN n.$3 = 1 THEN 'AMERICA'
    WHEN n.$3 = 2 THEN 'ASIA' 
    WHEN n.$3 = 3 THEN 'EUROPE'
    ELSE 'MIDDLE EAST'
  END AS REGION,
  SUBSTR(REGION, 1, 2) AS REGION_CODE,
  n.$2 AS NATION,
  n.$4 AS COMMENTS
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n;

-- 5.1.10 Load transformed data into target table
COPY INTO NATION
FROM (
  SELECT    
    n.$1 AS N_KEY,
    CASE 
      WHEN n.$3 = 0 THEN 'AFRICA' 
      WHEN n.$3 = 1 THEN 'AMERICA'
      WHEN n.$3 = 2 THEN 'ASIA' 
      WHEN n.$3 = 3 THEN 'EUROPE'
      ELSE 'MIDDLE EAST'
    END AS REGION,
    SUBSTR(REGION, 1, 2) AS REGION_CODE,
    n.$2 AS NATION,
    n.$4 AS COMMENTS
  FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n
);

-- 5.1.11 Verify load results
SELECT * FROM NATION;

-- 5.1.12 Right-size and suspend the warehouse
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER WAREHOUSE GATOR_WH SUSPEND;

-- 5.2.0 Key takeaways
-- - Transform on the fly within COPY by selecting from staged files
-- - Reorder and derive columns (CASE, SUBSTR, etc.)
-- - Use schema-local FILE FORMAT for predictable resolution

