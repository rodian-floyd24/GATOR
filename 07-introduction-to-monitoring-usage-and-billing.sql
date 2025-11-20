
-- 7.0.0   Introduction to Monitoring Usage and Billing
--         The purpose of this lab is to familiarize you with the Snowflake
--         database, the schemas in the database, and how you can use that data
--         to monitor how users are using the objects in your system and what
--         the associated costs are with that usage.
--         - Monitor usage and billing with the ACCOUNT_USAGE schema
--         - Determine which warehouses do not have resource monitors activated
--         for them
--         - Determine the most expensive queries from the last 30 days
--         - Determine the top 10 queries with the most spillage to remote
--         storage
--         HOW TO COMPLETE THIS LAB
--         As the workbook PDF may have useful diagrams, we recommend that you
--         read the instructions from the workbook PDF. In order to execute the
--         code presented in each step, use the SQL code file that was provided
--         for this lab.
--         OPENING THE SQL FILE
--         To load the SQL file, select Worksheets in the left navigation bar
--         and click the ellipses in the upper-right corner of your Snowsight
--         window. Select Create Worksheet from SQL File from the drop-down
--         menu. Navigate to the lab file for this lab and load it.
--         Snowflake Database
--         Snowflake provides a system-defined, read-only shared database named
--         SNOWFLAKE that contains metatdata and historical usage data about the
--         objects in your organization and account.
--         The purpose of the database is to allow you to monitor object usage
--         metrics as well as the costs associated with that usage so you can
--         make any adjustments needed to get the most for the credits being
--         spent.
--         There are many schemas in the SNOWFLAKE database, but the one we are
--         going to concentrate on is ACCOUNT_USAGE.
--         We have given you access to ACCOUNT_USAGE for learning purposes.
--         However, by default only users with the ACCOUNTADMIN can access the
--         SNOWFLAKE database and schemas, or perform queries on the views.
--         Privileges on the database can be granted to other roles in your
--         account to allow other users to access the objects.
--         Links to more information about the SNOWFLAKE database, and its other
--         schemas, can be found here:
--         Click here for Snowflake Database (https://docs.snowflake.com/en/sql-
--         reference/snowflake-db.html)
--         Let’s get started!

-- 7.1.0   Monitor Usage and Billing with the ACCOUNT_USAGE schema
--         The ACCOUNT_USAGE schema supports usage and billing monitoring
--         because it exposes a number of secure views that display data related
--         to object usage history, grants to roles and users, data loading
--         history, metering history, storage history, task history, users,
--         roles and more.
--         The main approach to writing queries that will help you monitor usage
--         and billing is to look at the views that appear relevant and then run
--         through the column list to see if they appear relevant to your goal.
--         As there are many views in each schema, some with a lot of columns,
--         it will take time and experience working with the schemas to become
--         sufficiently familiar with them to get the answers you want.

-- 7.1.1   Set your context:
--         For this lab, you will be using SNOWFLAKE.ACCOUNT_USAGE as your
--         database and schema.

USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS GATOR_WH INITIALLY_SUSPENDED=TRUE;
USE WAREHOUSE GATOR_WH;

USE SCHEMA SNOWFLAKE.ACCOUNT_USAGE;


-- 7.1.2   Examine credit usage by warehouse.
--         Monitoring credit consumption for specific objects is a classic use
--         of data in the ACCOUNT_USAGE schema. Here is an example of two
--         queries that use the WAREHOUSE_METERING_HISTORY view:

-- Credits used (all time = past year)
SELECT WAREHOUSE_NAME
      ,SUM(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE_SUM
  FROM ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
 GROUP BY 1
 ORDER BY 2 DESC;

-- Credits used (past N days/weeks/months)
SELECT WAREHOUSE_NAME,
       SUM(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE_SUM
  FROM ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
 WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())  // Past 7 days
 GROUP BY 1
 ORDER BY 2 DESC;

--         Notices the GROUP BY 1 and ORDER BY 2 in the query above. If your
--         column names are long, you can group or order by the column position,
--         rather than the column name.
--         These queries enable you to determine if there are specific
--         warehouses that are consuming more credits than the others. You can
--         then drill into questions such as: Should they be consuming that
--         quantity of credits? Are there specific warehouses that are consuming
--         more credits than anticipated?
--         In the event a warehouse is consuming too many credits you could take
--         action to rectify the sitution. Depending on what the warehouse is
--         being used for you could consider modifying the auto-suspend policy
--         or the scaling policy, checking the data loading history to see if
--         efficient practices are being used, or analyzing the size and
--         efficiency of queries being run on the warehouse, or adding a
--         resource monitor to the warehouse.
--         Below is a list of the columns in this view.
--         Since Snowflake is continually adding functionality, this could be
--         somewhat different from what you’ll see in the Snowsight User
--         Interface.

-- 7.1.3   Determining warehouses without resource monitors
--         If you have warehouses that are using too many credits, you can put
--         resource monitors on them.
--         The query below identifies all warehouses without resource monitors
--         in place. Resource monitors provide the ability to set limits on
--         credits consumed against a warehouse during a specific time interval
--         or date range. This can help prevent certain warehouses from
--         unintentionally consuming more credits than typically expected.


SHOW WAREHOUSES;

SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
 WHERE "resource_monitor" = 'null';


--         You shouldn’t find any warehouses without resource monitors because
--         all of them have at least one. So, your result should be 0. However,
--         if you have not put a resource monitor on your own warehouse
--         (GATOR_WH), then that warehouse may appear in the results.

-- 7.2.0   Billing Metrics
--         Billing metrics is all about analyzing what you’ve been billed in the
--         past so you can determine if there is a way to lower costs in the
--         future.

-- 7.2.1   Most expensive queries from the last 30 days
--         The query below analyzes queries that are potentially too expensive
--         by ordering the most expensive queries from the last 60 days. It
--         takes into account the warehouse size, assuming that a 1 minute query
--         on a larger warehouse is more expensive than a 1 minute query on a
--         smaller warehouse.


WITH WAREHOUSE_SIZE AS
(
     SELECT WAREHOUSE_SIZE, "CREDITS/HOUR"
       FROM (
              SELECT 'XSMALL' AS WAREHOUSE_SIZE, 1 AS "CREDITS/HOUR"
              UNION ALL
              SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS "CREDITS/HOUR"
              UNION ALL
              SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS "CREDITS/HOUR"
              UNION ALL
              SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS "CREDITS/HOUR"
              UNION ALL
              SELECT 'XLARGE' AS WAREHOUSE_SIZE, 16 AS "CREDITS/HOUR"
              UNION ALL
              SELECT '2XLARGE' AS WAREHOUSE_SIZE, 32 AS "CREDITS/HOUR"
              UNION ALL
              SELECT '3XLARGE' AS WAREHOUSE_SIZE, 64 AS "CREDITS/HOUR"
              UNION ALL
              SELECT '4XLARGE' AS WAREHOUSE_SIZE, 128 AS "CREDITS/HOUR"
            )
),
QUERY_HISTORY AS
(
     SELECT QH.QUERY_ID
           ,QH.QUERY_TEXT
           ,QH.USER_NAME
           ,QH.ROLE_NAME
           ,QH.EXECUTION_TIME
           ,QH.WAREHOUSE_SIZE
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH
     WHERE START_TIME > DATEADD(month,-2,CURRENT_TIMESTAMP())
)

SELECT QH.QUERY_ID
      ,'https://' || current_account() || '.snowflakecomputing.com/console#/monitoring/queries/detail?queryId='
            ||QH.QUERY_ID AS QU
      ,QH.QUERY_TEXT
      ,QH.USER_NAME
      ,QH.ROLE_NAME
      ,QH.EXECUTION_TIME as EXECUTION_TIME_MILLISECONDS
      ,(QH.EXECUTION_TIME/(1000)) as EXECUTION_TIME_SECONDS
      ,(QH.EXECUTION_TIME/(1000*60)) AS EXECUTION_TIME_MINUTES
      ,(QH.EXECUTION_TIME/(1000*60*60)) AS EXECUTION_TIME_HOURS
      ,WS.WAREHOUSE_SIZE
      ,WS."CREDITS/HOUR"
      ,(QH.EXECUTION_TIME/(1000*60*60))*WS."CREDITS/HOUR" as RELATIVE_PERFORMANCE_COST

FROM QUERY_HISTORY QH
JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
ORDER BY RELATIVE_PERFORMANCE_COST DESC
LIMIT 200;


--         This query gives you the chance to evaluate expensive queries and
--         take some action. For example, you could look at the query profile,
--         contact the user who executed the query, or take action to optimize
--         these queries.
--         Below is a list of the columns in this secure view,
--         SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY.

-- 7.2.2   Top 10 Queries With The Most Spillage to Remote Storage
--         Another way to evaluate the cost of queries is to see if they are
--         spilling to remote storage. The query below allows you to do that.


SELECT query_id, substr(query_text, 1, 50) partial_query_text, user_name, warehouse_name, warehouse_size, 
       BYTES_SPILLED_TO_REMOTE_STORAGE, start_time, end_time, total_elapsed_time/1000 total_elapsed_time
FROM   snowflake.account_usage.query_history
WHERE  BYTES_SPILLED_TO_REMOTE_STORAGE > 0
AND start_time::date > dateadd('days', -45, current_date)
ORDER BY BYTES_SPILLED_TO_REMOTE_STORAGE DESC
LIMIT 10;


--         Because you are in a training account, it is likely that you won’t
--         find any queries that have remote spillage. However, in a production
--         environment you will most likely encounter queries that have some
--         remote spillage.
--         This query also provides the warehouse name and size. Once you
--         identify the queries that are spilling to remote storage, you can
--         take action to ensure they are run on larger warehouses with more
--         local storage and memory.

-- 7.3.0   Key Takeaways
--         - There are many views in each schema of the Snowflake database, some
--         with a lot of columns. It will take time and experience working with
--         the schemas to become sufficiently familiar with them to get the
--         answers you want.
--         - Resource monitors provide the ability to set limits on credits
--         consumed against a warehouse during a specific time interval or date
--         range.
--         - The Snowflake database enables you to determine where you have high
--         credit consumption so you can ask pertinent questions as to why that
--         is occurring. You can use this information to help you consider steps
--         to bring down costs.
