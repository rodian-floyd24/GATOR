-- Visualizations in Snowsight (personalized, idempotent)
-- Uses: ROLE TRAINING_ROLE, WH GATOR_WH, SCHEMA SNOWBEARAIR_DB.PROMO_CATALOG_SALES

-- 2.1.0 Setup context (adjust role if needed)
USE ROLE TRAINING_ROLE;

-- Ensure a warehouse is available and selected
CREATE WAREHOUSE IF NOT EXISTS GATOR_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  INITIALLY_SUSPENDED = TRUE;
GRANT USAGE ON WAREHOUSE GATOR_WH TO ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;

-- Read-only lab data
-- If this fails, an admin must grant USAGE on database/schema to TRAINING_ROLE
USE SCHEMA SNOWBEARAIR_DB.PROMO_CATALOG_SALES;

-- 2.1.5 Query for year-by-year gross sales
-- Note: GROUP BY uses the expression for Snowflake compatibility
SELECT 
  YEAR(o.o_orderdate) AS year,
  SUM(l.l_extendedprice) AS sum_gross_revenue
FROM SNOWBEARAIR_DB.PROMO_CATALOG_SALES.CUSTOMER c
JOIN SNOWBEARAIR_DB.PROMO_CATALOG_SALES.ORDERS o
  ON c.c_custkey = o.o_custkey
JOIN SNOWBEARAIR_DB.PROMO_CATALOG_SALES.LINEITEM l
  ON o.o_orderkey = l.l_orderkey
GROUP BY YEAR(o.o_orderdate)
ORDER BY year;

-- 2.2.0 Use contextual statistics (UI):
--   - Open Query Details; explore YEAR and SUM_GROSS_REVENUE filters
--   - Click bars to filter, Clear filter to reset

-- 2.3.0 Create a Dashboard (UI):
--   - From worksheet menu: Move to -> + New Dashboard
--   - Name: GATOR Gross Sales
--   - Rename tile worksheet to: Gross Sales Data
--   - Return to dashboard to see the tile

-- 2.4.0 Add a chart tile (UI):
--   - On the data tile: ... -> Edit Query
--   - Click Chart; line graph displays
--   - Return to dashboard and arrange tiles as desired

-- 2.5.0 Share (UI):
--   - Click Share; invite Instructor1 (or your instructor’s user)

-- Optional checks
-- SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
-- SHOW GRANTS TO ROLE TRAINING_ROLE;
