-- Loading Structured Data (personalized, idempotent)
-- Uses: ROLE TRAINING_ROLE, WH GATOR_WH, DB GATOR_DB.PUBLIC
-- Notes:
--  - External stage is TRAINING_DB.TRAININGLAB.ED_STAGE (instructor-provided).
--  - If you lack USAGE on TRAINING_DB, ask your instructor to grant access.

-- 4.1.0 Setup context
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS GATOR_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE GATOR_WH;

CREATE DATABASE IF NOT EXISTS GATOR_DB;
USE SCHEMA GATOR_DB.PUBLIC;

-- 4.1.4 Create target table
CREATE OR REPLACE TABLE REGION (
  R_REGIONKEY NUMBER(38,0) NOT NULL,
  R_NAME      VARCHAR(25)  NOT NULL,
  R_COMMENT   VARCHAR(152)
);

-- 4.1.5/6 File formats for pipe-delimited and gzip pipe-delimited
CREATE OR REPLACE FILE FORMAT MYPIPEFORMAT
  TYPE = CSV
  COMPRESSION = NONE
  FIELD_DELIMITER = '|'
  FILE_EXTENSION = 'tbl'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT MYGZIPPIPEFORMAT
  TYPE = CSV
  COMPRESSION = GZIP
  FIELD_DELIMITER = '|'
  FILE_EXTENSION = 'tbl'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- 4.2.0 Inspect external stage and load uncompressed file
-- Stage details (delimiter on the stage format may differ from our override)
DESCRIBE STAGE TRAINING_DB.TRAININGLAB.ED_STAGE;

-- Peek file content (shows pipe-delimited columns)
SELECT n.$1
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/region.tbl n;

-- Confirm presence of files
LIST @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/ PATTERN='.*region.*';

-- Load region.tbl overriding the stage format with MYPIPEFORMAT
COPY INTO REGION
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/
FILES = ('region.tbl')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT);

SELECT * FROM REGION;

-- 4.3.0 Load gzip-compressed file
TRUNCATE TABLE REGION;

LIST @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/ PATTERN='.*region.*';

COPY INTO REGION
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/
FILES = ('region.tbl.gz')
FILE_FORMAT = (FORMAT_NAME = MYGZIPPIPEFORMAT);

SELECT * FROM REGION;

-- Optional: right-size and suspend warehouse
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER WAREHOUSE GATOR_WH SUSPEND;

-- 4.4.0 Validate data prior to load (NATION)
-- Create table with matching column order
CREATE OR REPLACE TABLE NATION (
  NATION_KEY INTEGER,
  NATION     VARCHAR,
  REGION_KEY INTEGER,
  COMMENTS   VARCHAR
);

-- Validate file (no load) – expect no errors in this matching layout
COPY INTO NATION
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/
FILES = ('nation.tbl')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
VALIDATION_MODE = RETURN_ALL_ERRORS;

-- Recreate table with swapped column order to induce errors
CREATE OR REPLACE TABLE NATION (
  NATION_KEY INTEGER,
  REGION_KEY INTEGER,
  NATION     VARCHAR,
  COMMENTS   VARCHAR
);

COPY INTO NATION
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/
FILES = ('nation.tbl')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
VALIDATION_MODE = RETURN_ERRORS;   -- returns all row-level errors without loading

-- Check only the first 10 rows for errors
COPY INTO NATION
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/
FILES = ('nation.tbl')
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
VALIDATION_MODE = RETURN_10_ROWS;

-- 4.5.0 Error handling during load
-- Restore matching table definition
CREATE OR REPLACE TABLE NATION (
  NATION_KEY INTEGER,
  NATION     VARCHAR,
  REGION_KEY INTEGER,
  COMMENTS   VARCHAR
);

-- Preview transformed data: convert REGION_KEY=1 to 'AMERICA' (string)
SELECT 
  n.$1 AS N_KEY,
  n.$2 AS NATION,
  CASE WHEN n.$3 = 1 THEN 'AMERICA' ELSE n.$3 END AS R_KEY,
  n.$4 AS COMMENTS
FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n;

-- ON_ERROR = CONTINUE (partial load)
COPY INTO NATION
FROM (
  SELECT 
    n.$1 AS N_KEY,
    n.$2 AS NATION,
    CASE WHEN n.$3 = 1 THEN 'AMERICA' ELSE n.$3 END AS R_KEY,
    n.$4 AS COMMENTS
  FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n
)
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = CONTINUE;

SELECT * FROM NATION;
TRUNCATE TABLE NATION;

-- ON_ERROR = ABORT_STATEMENT (default; full failure)
COPY INTO NATION
FROM (
  SELECT 
    n.$1 AS N_KEY,
    n.$2 AS NATION,
    CASE WHEN n.$3 = 1 THEN 'AMERICA' ELSE n.$3 END AS R_KEY,
    n.$4 AS COMMENTS
  FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n
)
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = ABORT_STATEMENT;

-- ON_ERROR = SKIP_FILE_4 (fail if ≥4 errors)
COPY INTO NATION
FROM (
  SELECT 
    n.$1 AS N_KEY,
    n.$2 AS NATION,
    CASE WHEN n.$3 = 1 THEN 'AMERICA' ELSE n.$3 END AS R_KEY,
    n.$4 AS COMMENTS
  FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n
)
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = SKIP_FILE_4;

SELECT * FROM NATION; -- should be empty after failure

-- ON_ERROR = SKIP_FILE_6 (partial load; 5 errors tolerated)
COPY INTO NATION
FROM (
  SELECT 
    n.$1 AS N_KEY,
    n.$2 AS NATION,
    CASE WHEN n.$3 = 1 THEN 'AMERICA' ELSE n.$3 END AS R_KEY,
    n.$4 AS COMMENTS
  FROM @TRAINING_DB.TRAININGLAB.ED_STAGE/load/lab_files/nation.tbl (FILE_FORMAT => 'MYPIPEFORMAT') n
)
FILE_FORMAT = (FORMAT_NAME = MYPIPEFORMAT)
ON_ERROR = SKIP_FILE_6;

SELECT COUNT(*) AS LOADED_ROWS FROM NATION; -- expect 20

-- 4.6.0 Key takeaways (reference)
-- - COPY INTO loads from stages with optional format overrides
-- - VALIDATION_MODE checks files without loading
-- - ON_ERROR controls failure/partial-load behavior
-- - LIST shows files; DESCRIBE STAGE shows stage settings

