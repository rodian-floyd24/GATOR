
-- 3.0.0   Caching and Query Performance
--         The purpose of this lab is to introduce you to the three types of
--         caching Snowflake employs and how you can use the Query Profile to
--         determine if your query is making the best use of caching.
--         - How to access and navigate the Query Profile.
--         - Understand the differences between metadata cache, query result
--         cache, and data cache.
--         - How to determine when and why the query result cache is being used
--         or not.
--         - How to determine if partition pruning is efficient, and if not, how
--         to improve it.
--         - How to determine if spillage is taking place.
--         - How to use EXPLAIN to determine how to improve your queries.
--         Like traditional relational database management systems, Snowflake
--         employs caching to help you get the query results you want as fast as
--         possible. Snowflake caching is turned on by default, and it works for
--         you in the background without you having to do anything. However, if
--         you aren’t sure if you’re writing queries that leverage caching in
--         the most efficient way possible, you can use the Query Profile to
--         determine how caching is impacting your queries.
--         Imagine that you’re a data analyst at Snowbear Air and that you’ve
--         just learned that Snowflake has different types of caching. You have
--         been working on a few queries that you think could be running faster,
--         but you’re not sure. You’ve decided to become familiar with the Query
--         Profile and plan to use it to see how caching is impacting your
--         queries.
--         HOW TO COMPLETE THIS LAB
--         In order to complete this lab, you can type the SQL commands below
--         directly into a worksheet. It is not recommended that you cut and
--         paste from the workbook pdf as that sometimes results in errors.
--         You can also use the SQL code file for this lab that was provided at
--         the start of the class. To open an .SQL file in Snowsight, make sure
--         the Worksheet section is selected on the left navigation bar. Click
--         on the ellipsis between the Search and +Worksheet buttons. In the
--         dropdown menu, select Create Worksheet from SQL File.
--         Let’s get started!

-- 3.1.0   Accessing and Navigating the Query Profile
--         In this section, you’ll learn how to access, navigate and use the
--         Query Profile. This will prepare you for analyzing query performance
--         and caching in this lab.

-- 3.1.1   Create a new folder and call it Caching and Query Performance.

-- 3.1.2   Create a new worksheet inside the folder and call it Working with
--         Cache and the Query Profile.

-- 3.1.3   Set the worksheet context as shown below:
--         We’ll also alter our session to avoid using cached results from
--         previous sessions. This will allow us to see what the cache really is
--         doing going forward.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;
CREATE DATABASE IF NOT EXISTS GATOR_DB;
USE SCHEMA GATOR_DB.PUBLIC;

--This will clear the data from previous sessions that is cached in the virtual warehouse
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;


-- 3.1.4   Create some data to query.
--         Here we’ll create a table and populate it with data from the customer
--         table. We’ll also suspend the virtual warehouse to make sure our
--         subsequent query doesn’t just pull from cached data.

CREATE TABLE customer AS
SELECT 
       c_custkey
     , c_firstname
     , c_lastname
  FROM 
       SNOWBEARAIR_DB.PROMO_CATALOG_SALES.customer;

ALTER WAREHOUSE GATOR_WH SUSPEND;


-- 3.1.5   Execute the following simple query in your worksheet.
--         We need query results to view in the profile. Run the statement
--         below.

SELECT DISTINCT
        *
FROM 
       customer c;


-- 3.1.6   Access the Query Profile.
--         Now let’s view the profile. To the right of the query results, you
--         will see a panel saying, Result limit exceeded….. Click the X in the
--         upper right of this panel to show the usual Query Details panel.
--         Click the ellipses shown in the screenshot. When the dialog box
--         appears, click View Query Profile. The Query Profile will open in a
--         new tab.
--         Please note that what you may see might differ slightly from what is
--         shown here. Since you are working in an environment that is not the
--         same as the environment used to create the screenshots shown here,
--         these differences are normal and to be expected.
--         The query profile will appear as shown below:
--         Note that there are two tabs in the header of the screen: Query
--         Details and Query Profile.

-- 3.1.7   Click the query details tab.

-- 3.1.8   Click the query profile tab.
--         Note that there are two panels in the query profile that show
--         specific aspects of execution.
--         NOTE Your results may differ from what is shown in the screenshots
--         below.
--         Note that 70% of the execution time was spent setting up the query
--         (Initialization), 10% was blocked by remote disk access (Remote Disk
--         I/O), and 20% was spent on processing data for the query by the CPU
--         (Processing). Most of the execution time (which was just slightly
--         over 1 second) was spent in Initialization due to the fact we were
--         running a very simple query against a small amount of data. If we had
--         been running a more complex query against more data and the execution
--         time was not satisfactory, finding a high disk I/O would tell us we
--         need to do something to reduce that, such as filtering.

-- 3.1.9   Click on Node A (TableScan[2]).
--         This node shows statistics related to the scan of the customer table.
--         As we saw in a previous step, the Statistics panel shows that one
--         partition was scanned out of one total partitions evaluated. The
--         query was very simple, and the data set isn’t very large, so this was
--         to be expected.
--         Note that an Attributes panel appears that shows the columns selected
--         during processing of this node’s query.

-- 3.1.10  Click on Node B (Aggregate[1]).
--         This node shows the statistics related to the aggregation of the
--         data.
--         NOTE: You should also get similar results, but it’s possible yours
--         may differ slightly.

-- 3.1.11  Click on Node C (Result[3]).
--         This node shows the columns that were in the output.
--         Note that an Attributes panel appears that shows the columns produced
--         by the processing of this node.

-- 3.1.12  Close the tab.
--         You’ll remember that the Query Profile opened up in a new tab. While
--         we could navigate back to our folder, that would leave us with two
--         tabs open. Close this tab and return to the tab with your worksheet.

-- 3.1.13  Go back to the tab with your worksheet and run your query again.
--         Once you’ve run the query again, you should notice that it ran in a
--         few milliseconds, much faster than before. Now let’s look at the
--         Query Profile again to see what happened.
--         As you can see, the query gave us the exact same result in just a few
--         milliseconds because it was serving us the same results from the
--         query result cache in the cloud services layer. This is a ready-to-go
--         feature that you don’t have to think about. You can just run your
--         query a second time and get the same results again if needed.
--         Close the Query Profile tab and return to your worksheet.

-- 3.2.0   Metadata Cache
--         Metadata cache is simple. When data is written into Snowflake
--         partitions, the MAX, MIN, COUNT, and other values are stored in the
--         metadata cache in the Cloud Services layer. This means that when your
--         query needs these values, rather than scanning the table and
--         calculating the values, it simply pulls them from the metadata cache.
--         This makes your query run much faster. Let’s try it out!

-- 3.2.1   Scenario.
--         Let’s imagine you’ve been asked to analyze part and supplier data.
--         We’re going to use the PARTSUPP table in our database called
--         SNOWFLAKE_SAMPLE_DATA because it provides enough data for this
--         exercise.

-- 3.2.2   Set the context.
--         Note we’re setting USE_CACHED_RESULT = FALSE in order to avoid using
--         the query result cache.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF10;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;


-- 3.2.3   Run the following SQL statement:

SELECT 
          MIN(ps_partkey)
        , MAX(ps_partkey) 
        
FROM 
        PARTSUPP;

--         Now check the Query Profile. You should see a single node that says
--         METADATA-BASED RESULT. This is because the query profile simply went
--         to cache to get the data you needed rather than scanning a table. So,
--         there was no disk I/O at all.
--         Close the Query Profile tab and return to your worksheet.

-- 3.3.0   Data Cache
--         Snowflake caches data from queries you run so it can be accessed
--         later by other queries. This cache is saved to disk in the virtual
--         warehouse. Let’s take a look at how it works. Once again, let’s
--         assume you’ve been asked to analyze part and supplier data.

-- 3.3.1   Clear your cache.
--         With the first statement below, you will ensure the query result
--         cache is disabled so that you can focus on the data cache. With the
--         remaining statements, you’ll suspend and restart your virtual
--         warehouse to ensure you’re not using previously cached data.

ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;


-- 3.3.2   Run the SQL statement below.
--         Let’s start by selecting two columns, ps_suppkey and ps_availqty,
--         with a WHERE clause that selects only part of the dataset. For any
--         rows the query retrieves, this will cache the data for the two
--         columns plus the column in the WHERE clause.

SELECT 
          ps_partkey
        , ps_availqty
        
FROM 
        PARTSUPP
        
WHERE 
        ps_partkey > 1000000; 


-- 3.3.3   Look at percentage scanned from cache under Statistics in the Query
--         Profile.
--         You should see that the percentage scanned from cache is 0.00%. This
--         is because we just ran the query for the first time on a newly
--         resumed virtual warehouse.

-- 3.3.4   Run the query again.

SELECT 
          ps_partkey
        , ps_availqty
        
FROM 
        PARTSUPP
        
WHERE 
        ps_partkey > 1000000; 


-- 3.3.5   Look at percentage scanned from cache under Statistics in the Query
--         Profile.
--         You should see that the percentage scanned from cache is 100.00%.
--         This is because the query was able to get 100% of the data it needed
--         from the data cache. This results in faster performance than a query
--         that does a lot of disk I/O.

-- 3.3.6   Add columns, run the query, and check the Query Profile.

SELECT 
          ps_partkey
        , ps_suppkey
        , ps_availqty
        , PS_supplycost
        , ps_comment
        
FROM 
        PARTSUPP
        
WHERE 
        ps_partkey > 1000000;  

--         When you check the percentage scanned from cache, it should be less
--         than 100%. This is because we added columns that weren’t fetched
--         previously, so some disk I/O must occur in order to fetch the data in
--         those columns.
--         Close the Query Profile tabs and return to your worksheet.

-- 3.4.0   Partition Pruning
--         Partition pruning is a process by which Snowflake eliminates
--         partitions from a table scan based on the partition’s metadata. What
--         this means is that fewer partitions are read from the storage layer
--         or are involved in filtering and joining, which gives you faster
--         performance.
--         Data in Snowflake tables will be organized based on how the data is
--         ingested. For example, if the data in a table has been organized
--         based on a particular column, knowing which column that is and
--         including it in joins or in WHERE clause predicates will result in
--         more partitions being pruned and, thus, faster query performance.
--         This organization is called natural clustering. As the details
--         related to clustering, in general, are not within the scope of this
--         course, you can learn more by checking our documentation or by taking
--         the Snowflake Advanced course.
--         Let’s look at an example. We’re going to be using a different and
--         larger dataset than our PROMO_CATALOG_SALES dataset so we can
--         leverage partition pruning. Let’s set the context, set our virtual
--         warehouse size to xsmall, and clear our data cache.

-- 3.4.1   Set the context, virtual warehouse size, and clear your cache.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER SESSION SET USE_CACHED_RESULT=FALSE;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;

--         Remember, if your virtual warehouse is already suspended, either by
--         timeout or command, you will receive the following error message upon
--         execution of your SUSPEND statement: “Invalid state. Warehouse
--         GATOR_WH cannot be suspended”.

-- 3.4.2   Execute a query with partition pruning.
--         Let’s imagine that the Snowbear Air marketing team has asked you for
--         a list of customer addresses via a join on the CUSTOMER and
--         CUSTOMER_ADDRESS tables. The data in the CUSTOMER table has been
--         organized based on C_CUSTOMER_SK, which is a unique identifier for
--         each customer. The WHERE clause filters on both C_CUSTOMER_SK and on
--         C_LAST_NAME. Execute the query below and check the Query Profile to
--         see what happens.

SELECT  
          C_CUSTOMER_SK
        , C_LAST_NAME
        , (CA_STREET_NUMBER || ' ' || CA_STREET_NAME) AS CUST_ADDRESS
        , CA_CITY
        , CA_STATE  
FROM 
          CUSTOMER
          INNER JOIN CUSTOMER_ADDRESS ON C_CUSTOMER_ID = CA_ADDRESS_ID
WHERE 
        C_CUSTOMER_SK between 100000 and 600000
        AND
        C_LAST_NAME LIKE 'Johnson' 
ORDER BY 
          CA_CITY
        , CA_STATE;

--         If you check the nodes for each table, you’ll see that the CUSTOMER
--         and CUSTOMER_ADDRESS tables have just over 500 total partitions
--         between them.
--         The Query Profile tells us that the query ran in a few seconds, and
--         only about half of the partitions were scanned. So this query ran
--         faster than it would have otherwise because partition pruning worked
--         for us.
--         Now let’s run a query without the C_CUSTOMER_SK field in the WHERE
--         clause predicate and see what happens.

-- 3.4.3   Execute a query without partition pruning and check the Query
--         Profile.

SELECT  
          C_CUSTOMER_SK
        , C_LAST_NAME
        , (CA_STREET_NUMBER || ' ' || CA_STREET_NAME) AS CUST_ADDRESS
        , CA_CITY
        , CA_STATE  
FROM 
          CUSTOMER
          INNER JOIN CUSTOMER_ADDRESS ON C_CUSTOMER_ID = CA_ADDRESS_ID
WHERE 
        C_LAST_NAME = 'Johnson' 
ORDER BY 
          CA_CITY
        , CA_STATE;

--         The Query Profile tells us that this query took longer to run, and
--         all partitions were scanned. This is because the data in the CUSTOMER
--         table is not organized on the C_LAST_NAME column, so more partitions
--         had to be scanned in order for us to get our query result. The
--         takeaway here is that understanding how your table’s data is
--         organized can help you write more efficient queries. Your DBA
--         (Snowflake Administrator) is someone who can help you find that
--         information.

-- 3.5.0   Determine if Spillage is Taking Place
--         Now let’s determine if spillage is taking place in one of our
--         queries. Spillage means that because an operation cannot fit
--         completely in memory, data is spilled to disk within the virtual
--         warehouse. This is known as local spillage. When the disk in the
--         virtual warehouse is full, the operation begins to spill to storage.
--         This is known as remote spillage. Operations that incur spillage are
--         slower than memory access and can greatly slow down query execution.
--         Thus, you need to be able to identify and rectify spillage.
--         Let’s imagine that Snowbear Air wants to determine the average list
--         price, average sales price, and average quantity for both male and
--         female buyers in the year 2000 for the months of January through
--         October.
--         Rather than use our PROMO_CATALOG_SALES database for this scenario,
--         we’re going to use another database that has enough data to create
--         spillage. The structure and content of the data is less important
--         than the fact that we can generate and resolve a spillage issue.

-- 3.5.1   Set the context and resize the virtual warehouse.

USE ROLE TRAINING_ROLE;
USE WAREHOUSE GATOR_WH;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';


-- 3.5.2   Clear the cache.
--         Run the SQL statements below to set USE_CACHED_RESULT to false, and
--         suspend and resume your virtual warehouse to clear any cache. This
--         will ensure we get spillage when we run our query.

ALTER SESSION SET USE_CACHED_RESULT=FALSE;
ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;


-- 3.5.3   Run a query that generates spillage.
--         Note that the query below has a nested query. The nested query
--         determines the average list price, average sales price, and average
--         quantity per gender type and order number. The outer query then
--         aggregates those values by gender.
--         Run the query below. It should take around 3 to 5 minutes to run
--         (your results may vary).

SELECT 
          cd_gender
        , AVG(lp) average_list_price
        , AVG(sp) average_sales_price
        , AVG(qu) average_quantity
FROM 
        (
          SELECT 
                  cd_gender
                , cs_order_number
                , AVG(cs_list_price) lp
                , AVG(cs_sales_price) sp
                , AVG(cs_quantity) qu
          FROM 
                  catalog_sales
                , date_dim
                , customer_demographics
          WHERE 
                cs_sold_date_sk = d_date_sk
                AND 
                cs_bill_cdemo_sk = cd_demo_sk
                AND 
                d_year IN (2000) 
                AND 
                d_moy IN (1,2,3,4,5,6,7,8,9,10)
         GROUP BY 
                  cd_gender
                , cs_order_number
        ) inner_query
GROUP BY 
        cd_gender;


-- 3.5.4   View the results.
--         For female buyers, you should see something very similar to the
--         following figures:
--         For male buyers, you should see something very similar to the
--         following figures:

-- 3.5.5   Check out the Query Profile.
--         Go to the Query Profile. As you will see at the bottom of the
--         Statistics, gigabytes of data were spilled to local storage. Notice
--         there are two Aggregate nodes in the query. Click on each and notice
--         the first Aggregate node is where the spillage is happening. This
--         node is part of the inner query. Let’s rectify this issue by
--         rewriting our query.
--         If you look back at the query you just ran, you’ll see that the outer
--         query is not really necessary. All you need to do is remove the
--         cs_order_number column from the nested query and then run it.

-- 3.5.6   Run the modified nested query.
--         Let’s run the query. We’ll suspend the virtual warehouse first to
--         flush any cache so we can get a true reading of how long it will take
--         for the query to run.

ALTER WAREHOUSE GATOR_WH SUSPEND;
ALTER WAREHOUSE GATOR_WH RESUME;

SELECT 
         cd_gender
       , AVG(cs_list_price) lp
       , AVG(cs_sales_price) sp
       , AVG(cs_quantity) qu
FROM 
        catalog_sales
      , date_dim
      , customer_demographics
WHERE 
      cs_sold_date_sk = d_date_sk
      AND 
      cs_bill_cdemo_sk = cd_demo_sk
      AND 
      d_year IN (2000) 
      AND 
      d_moy IN (1,2,3,4,5,6,7,8,9,10)
GROUP BY 
          cd_gender;


-- 3.5.7   Check your results.
--         The query should have run in 1-3 minutes (your results may vary).
--         Compare your results to the ones you got previously. They may be
--         slightly different past the hundreds place to the right of the
--         decimal, but that is due to the differences in rounding between the
--         original query and the modified nested query. So, in essence, you got
--         the same results only in far less time.

-- 3.5.8   Check the Query Profile.
--         As you will see at the bottom of the Statistics, there is no longer a
--         spillage entry. This means that you resolved your spillage issue by
--         simply rewriting your query so that it was more efficient.

-- 3.6.0   Review the EXPLAIN Plan
--         Now let’s compare the EXPLAIN plans from both of the queries we just
--         ran in order to see how they are different.

-- 3.6.1   Use EXPLAIN to see the plan for the first query.

EXPLAIN
SELECT 
          cd_gender
        , AVG(lp) average_list_price
        , AVG(sp) average_sales_price
        , AVG(qu) average_quantity
FROM 
        (
          SELECT 
                  cd_gender
                , cs_order_number
                , AVG(cs_list_price) lp
                , AVG(cs_sales_price) sp
                , AVG(cs_quantity) qu
          FROM 
                  catalog_sales
                , date_dim
                , customer_demographics
          WHERE 
                cs_sold_date_sk = d_date_sk
                AND 
                cs_bill_cdemo_sk = cd_demo_sk
                AND 
                d_year IN (2000) 
                AND 
                d_moy IN (1,2,3,4,5,6,7,8,9,10)
          
          GROUP BY 
                  cd_gender
                , cs_order_number
        ) inner_query
GROUP BY 
        cd_gender;


-- 3.6.2   Click on the operation column header and select the up arrow to sort
--         the rows alphabetically by operation type.
--         Note that there are 12 rows that correspond to the execution nodes
--         that you would see in the Query Profile. Also, note that two of the
--         rows are aggregate rows. The node below executes the averaging of the
--         list price, sales price, and quantity:

-- 3.6.3   Run the EXPLAIN statement for the second query.

EXPLAIN        
SELECT 
         cd_gender
       , AVG(cs_list_price) lp
       , AVG(cs_sales_price) sp
       , AVG(cs_quantity) qu
FROM 
        catalog_sales
      , date_dim
      , customer_demographics
WHERE 
      cs_sold_date_sk = d_date_sk
      AND 
      cs_bill_cdemo_sk = cd_demo_sk
      AND 
      d_year IN (2000) 
      AND 
      d_moy IN (1,2,3,4,5,6,7,8,9,10)
GROUP BY 
          cd_gender;

--         Notice now that this plan is identical to the first one except that
--         there is one aggregate row fewer than in the previous explain plan
--         (for a total of 11 rows). Specifically, the node shown in the
--         previous step in this lab is the one that is gone because we removed
--         the outer query. Making that change alone was enough to cut query
--         time by more than half.

-- 3.6.4   Change your virtual warehouse size to XSmall.

ALTER WAREHOUSE GATOR_WH
    SET WAREHOUSE_SIZE = 'XSmall';
ALTER WAREHOUSE GATOR_WH SUSPEND;


-- 3.7.0   Summary
--         Writing efficient queries is an art that takes a solid understanding
--         of how Snowflake caching and query pruning impact query performance.
--         While it’s impossible to show you every single scenario, you should
--         know that getting proficient at using tools like the Query Profile
--         and the EXPLAIN plan will help you better understand how caching
--         impacts your query performance. This, in turn, will allow you to
--         write better queries that achieve a shorter run time.

-- 3.8.0   Key takeaways
--         - Snowflake employs caching to help you get the query results you
--         want as fast as possible.
--         - Cache is turned on by default, and it works for you in the
--         background without you having to do anything.
--         - The Query Profile is a useful tool for understanding how caching
--         and partition pruning are impacting your queries.
--         - As you add or remove columns to/from a SELECT clause or a WHERE
--         clause, your percentage scanned from cache value could go up or down.
--         - If your query only requests MIN or MAX values on INTEGER, DATE or
--         DATETIME data types, those values will come from metadata cache in
--         the Cloud Services layer rather than from disk I/O, which results in
--         fast performance.
--         - Query Result cache is invoked when you run the exact same query
--         twice.
--         - Data cache resides in the virtual warehouse, and it stores data
--         from past queries on a least recently used (LRU) basis, until the
--         virtual warehouse is suspended. However, once the virtual warehouse
--         is suspended, its data cache is cleared out.
--         - Including the column on which a table’s data is organized in a
--         WHERE clause predicate can improve partition pruning, which in turn
--         improves performance.
--         - Using EXPLAIN can give you insight into how Snowflake will execute
--         your query. You can use it to identify and remove bottlenecks in your
--         query so you can resolve them and get better efficiency.
