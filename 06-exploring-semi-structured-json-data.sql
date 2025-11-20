
-- 6.0.0   Exploring Semi-Structured JSON Data
--         In this lab, you will create a simple table that includes a VARIANT
--         column. You will work with that simple table before you go on to
--         explore more complex data using both dot notation syntax and the
--         FLATTEN function.
--         By the end of this lab, you will be able to:
--         - Create a simple table with JSON data
--         - Query a column containing JSON data using dot notation syntax
--         - Query a column containing JSON data using the FLATTEN function
--         - Determine the data types of a query result
--         Snowbear Air has started to collect some data in JSON format, but
--         none of the analysts are familiar with using semi-structured data.
--         The management team at Snowbear Air has tasked you with learning
--         about this new data type, and teaching the other analysts what you
--         have learned. You decide to begin by creating, and then working with,
--         some simple JSON data.
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
--         Let’s get started!

-- 6.1.0   Query Simple JSON Data Using dot notation syntax

-- 6.1.1   Make sure you have the required objects, and set your context for
--         this lab:

USE ROLE training_role;

CREATE WAREHOUSE IF NOT EXISTS GATOR_WH;
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE=XSmall;
USE WAREHOUSE GATOR_WH;

CREATE DATABASE IF NOT EXISTS GATOR_DB;
USE DATABASE GATOR_DB;

CREATE SCHEMA IF NOT EXISTS GATOR_SCHEMA;
USE SCHEMA GATOR_SCHEMA;


-- 6.1.2   Create a simple JSON file.
--         Use the SQL provided to create a simple table with two columns. The
--         first column is a customer ID number, and the second column contains
--         a JSON record with information about the customer. It uses the
--         parse_json function to create the JSON data. It also uses the $1
--         notation to refer to the first column in the table, and $2 to refer
--         to the second column.

CREATE OR REPLACE TABLE customers AS
SELECT
   $1 AS id,
   parse_json($2) AS info
FROM
   VALUES
      (12712555, '{"name": {"first": "John", "last":"Smith"}}'),
      (98127771, '{"name": {"first": "Jane", "last":"Doe"}}');


-- 6.1.3   Open a row of the table you just created to see its structure.

SELECT * FROM customers;

--         Click on a row in the info column to display its structure on the
--         right. Notice that the top-level key is called name, and its value is
--         an object (a collection of key-value pairs).

-- 6.1.4   Extract the name data from your table using dot notation syntax.
--         One way to extract values from JSON data is with dot notation syntax.
--         Dot Notation Syntax:
--         In your SELECT statement, you would simply reference the column, add
--         a colon, then reference the elements one-by-one separated by dots:
--         <column>:<level1_element>.<level2_element>.<level3_element>
--         Run the SELECT statement below to see dot notation syntax at work:

SELECT
   id,
   info:name.first AS first_name,
   info:name.last AS last_name
FROM
   customers;

--         For first_name, we reference column info, then follow it with a
--         colon, then level 1 element name, then level 2 element first. We do
--         something similar for last_name.

-- 6.1.5   Determine the data types in your result set.
--         Below we’ll use the command DESCRIBE RESULT and the function
--         last_query_id() to fetch details about the query we just ran.

DESCRIBE RESULT last_query_id();

--         Notice that the name columns are still of data type VARIANT. Let’s
--         look at another way to determine if our data is of data type VARIANT.

-- 6.1.6   Rerun the query examine the output

SELECT
   id,
   info:name.first AS first_name,
   info:name.last AS last_name
FROM 
   customers;

--         You’ll notice that the first_name and last_name column values are
--         enclosed in double-quotes. Seeing string values in double-quotes is
--         an indication that the output is probably still a VARIANT.
--         While you can use VARIANT data types in comparisons or aggregations
--         with other data types, your queries won’t be as performant as they
--         would be if you had cast the VARIANT columns to standard SQL data
--         types.
--         Let’s observe how we can convert VARIANTS to a standard SQL data
--         type.

-- 6.1.7   Extract the name data and cast it to type VARCHAR.

SELECT
   id,
   info:name.first::VARCHAR AS first_name,
   info:name.last::VARCHAR AS last_name
FROM
   customers;

DESCRIBE RESULT last_query_id();

--         As you can see, your first_name and last_name columns are now VARCHAR
--         values. This allows you to sort them, apply string functions to them,
--         or do anything you could with structured data.

-- 6.1.8   Create a table from your query

CREATE OR REPLACE TABLE customers_structured
AS
   SELECT 
      id,
      info:name.first::VARCHAR AS first_name,
      info:name.last::VARCHAR AS last_name
   FROM
      customers;

SELECT * FROM customers_structured;

--         You now have a table that can be used for analysis by your colleagues
--         who do not know how to directly query semi-structured data.

-- 6.2.0   Query Nested JSON Data with Dot Notation
--         There are two ways to query nested JSON data: one is with dot
--         notation, and the other is with the FLATTEN function. Let’s look at
--         dot notation first.

-- 6.2.1   Create a new version of your customers table with various levels of
--         nested JSON data
--         The table below has various levels of nested values. As you can see,
--         contact has both business and personal details. Key-value pairs phone
--         and email are nested within both business and personal.
--         Run the code below to create the table.

CREATE OR REPLACE TABLE customers 
AS 
SELECT
   $1 AS id,
   parse_json($2) AS info
FROM VALUES
   (
   12712555,
   '{"name": {"first":"John", "last":"Smith"},
     "contact": [
        {"business": {"phone":"303-555-1234", "email":"j.smith@company.com"}},
        {"personal": {"phone":"303-421-8322", "email":"jsmith332@gmail.com"}}
        ],
     }'
   ),
   (
   98127771,
   '{"name": {"first":"Jane", "last":"Doe"},
     "contact": [
        {"business": {"phone":"303-638-4887", "email":"jg_doe@company2.com"}},
        {"personal": {"phone":"303-678-6789", "email":"happyjane@gmail.com"}}
        ],
     }'
   );

--         Notice that the value for key contact is enclosed in square brackets.
--         This is because the value is an array of key value pairs, each of
--         which contains two key-value pairs. Whenever you see those square
--         brackets, you will need to do something extra with your dot notation
--         in order to fetch the values you want.

-- 6.2.2   Select from your table and click a cell in the INFO column to view
--         the structure.

SELECT * FROM customers;

--         Notice that there are two people: John Smith and Jane Doe. Each has a
--         key-value called contact, with business and personal contact details
--         nested within. Let’s query the table to fetch both people and their
--         business and personal phone numbers using the same dot notation
--         syntax we’ve been using.

-- 6.2.3   Query the table to fetch business and personal phone numbers

SELECT
   ID,
   info:name.first::VARCHAR AS first_name,
   info:name.last::VARCHAR AS last_name,
   info:contact.business.phone::VARCHAR AS business_phone,
   info:contact.personal.phone::VARCHAR AS personal_phone
FROM
   customers;


--         Notice that while our query fetched the full names of both John and
--         Jane, the business phone and personal phone columns are null.
--         The key contact has a value consisting of a zero-based array of
--         elements, each of which is a key-value pair. Because the keys
--         business and personal, along with their values are nested within an
--         array, we can’t simply query info:contact.business and expect to get
--         the business phone number. That’s because we haven’t told the query
--         engine which element in the array we want, the first or the second.
--         Thus, in order to access the value of business (the first element in
--         the array) we have to give the query engine an integer indicating a
--         position in the array (an index). In our example the positions are 0
--         for business and 1 for personal. Remember, the first position is 0
--         because the array is zero-based.
--         Run the SELECT statement below to see this in action.

SELECT
   ID,
   info:name.first::VARCHAR AS first_name,
   info:name.last::VARCHAR AS last_name,
   info:contact[0].business.phone::VARCHAR AS business_phone,
   info:contact[1].personal.phone::VARCHAR AS personal_phone
FROM
   customers;

--         As you can see, we indicate contact[0] for the business phone number
--         and contact[1] for the personal phone number. Thus, the query engine
--         understood which path it needed to follow. It then followed the dot
--         notation (business.phone and personal.phone) to fetch the values we
--         wanted.

-- 6.2.4   Fetch all data and put it in structured format:

CREATE OR REPLACE TABLE customers_dot
AS
SELECT
   ID,
   info:name.first::VARCHAR AS first_name,
   info:name.last::VARCHAR AS last_name,
   info:contact[0].business.phone::VARCHAR AS business_phone,
   info:contact[0].business.email::VARCHAR AS business_email,
   info:contact[1].personal.phone::VARCHAR AS personal_phone,
   info:contact[1].personal.email::VARCHAR AS personal_email
FROM
   customers;

SELECT * FROM customers_dot;   

--         As you can see, we’ve now converted our semi-structured data to a
--         structured format so it can be used by colleagues that need to join
--         it with other structured data, or that don’t know how to work with
--         semi-structured data.

-- 6.3.0   Use FLATTEN to Query JSON Data
--         Now we’re going to use the FLATTEN function (instead of dot notation)
--         to produce a structured representation of the same semi-structured
--         data we were working with earlier.
--         FLATTEN is a table function that explodes the contents of a variant
--         column (for purposes of this lab, a column with semi-structured data)
--         and produces a table-like representation of the data.
--         The nice thing about the FLATTEN function is that it gives you some
--         additional output fields that you can use to figure out how your data
--         is structured and to fine-tune your query to get the results that you
--         want.

-- 6.3.1   Use the FLATTEN function to view all the top-level keys from your
--         table.
--         The top-level keys are the first level of key-value pairs that the
--         query engine finds when parsing the semi-structured data indicated in
--         your query.
--         Run the query below to find those keys.

SELECT 
   * 
FROM 
   customers,
LATERAL FLATTEN(input=>info);

--         When we use the FLATTEN function, its output is in essence joined to
--         table customers and its columns can be selected just as you would
--         with columns from a table of structured data.
--         First, notice that the input of the FLATTEN function is
--         (input=\>\<column\>). For `<column>’ you can use just the column name
--         or a dot notation path like the ones we saw earlier in this lab.
--         Now notice that your output contains eight columns. The first two (ID
--         and INFO) are columns from the customers table, and the last six are
--         the output of the FLATTEN function. Look at the key column - this
--         lists all of the top-level keys in the INFO column.
--         This might be a little easier to see if you just pull out the key and
--         value columns:

SELECT
   key,
   value
FROM
   customers,
LATERAL FLATTEN(input=>info);

--         You get four rows of output: the first two rows are the top-level
--         keys (contact and name) from John Smith’s record, and the second two
--         rows are the same keys from Jane Doe’s record. But what if you want
--         to see all the keys, even those nested in other structures?

-- 6.3.2   Use the RECURSIVE option to view all the keys from your table.
--         This time you will include the index:

SELECT 
   key, index, value  
FROM 
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true);

--         Note that you have gone from four rows of output to 24. When you
--         include the RECURSIVE option, any time a value is either an object or
--         an array (as opposed to a simple value), then that object or array is
--         also flattened.
--         Note that when the index column shows 0, the value column shows the
--         business details. When the index column shows 1, the value column
--         shows the personal details. This is consistent with what we saw in
--         the earlier dot notation exercise.

-- 6.3.3   Show only the keys that have simple values.
--         When you query semi-structured data, you are typically accessing keys
--         that have simple values. For example, you’re looking for the business
--         phone, rather than the entire array of contact information. You can
--         do this with a WHERE clause that uses the function TYPEOF to check
--         the type of value being returned.
--         In the query below, you will check the contents of the value column
--         from the FLATTEN.

SELECT
   key, index, value
FROM
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

--         Notice that the index column contains nothing but null values. This
--         is because the index column tells you the position of values in an
--         array - but you filtered out all of the arrays with your WHERE
--         clause. So there’s no point in including the index column in the
--         query.

SELECT
   key, value
FROM
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

--         Now you know all the simple key:value pairs in each row, but you may
--         still have some questions about the data. For example, several of the
--         rows have a key of phone - but is that a business phone, or a
--         personal phone? And is it for John or for Jane? The columns this,
--         seq, and path from the FLATTEN function can help answer these
--         questions.

-- 6.3.4   Include the column this in your query.

SELECT
   key, value, this
FROM
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

--         The column named this shows you what is being flattened for each row.
--         This really only has value if you are doing a recursive flatten
--         (otherwise, this will always show the input to the FLATTEN function).
--         If you look at the this column across from each phone number, you
--         have a good idea of who the phone number is for based on their email
--         address. But email addresses do not always include someone’s name…so
--         that is not the best indicator.

-- 6.3.5   Include the path column in your query.

SELECT
   key, value, path
FROM
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

--         The path column is essentially the dot notation syntax representation
--         of how you got down to the value displayed. For a key of phone, you
--         can see that the path tells you whether it is a personal phone or a
--         business phone. But that still doesn’t solve the problem of who that
--         phone number belongs to.

-- 6.3.6   Include the seq column in your query.

SELECT
   seq, key, value 
FROM
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

--         Each row of JSON data is assigned a sequence number. So when you see
--         multiple rows of output, you know that all rows with the same
--         sequence value came out of the same row of data. Now it’s easy to
--         match up phone numbers with the owner - you just look for the first
--         and last name with the same sequence number as the phone. And the
--         path will tell you whether it is a personal phone, or a business
--         phone.
--         In your output, it is very likely that the first six rows have a
--         sequence value of 1, and the last six rows have a sequence value of
--         2. However, sequence numbers are not guaranteed to be displayed in
--         order. So you should order your output by sequence number if you want
--         to be sure that all the values from a single row are displayed
--         together:

SELECT
   seq, key, value 
FROM
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true)
WHERE IS_ARRAY(value)='false' AND IS_OBJECT(value)='false'
ORDER BY seq;

--         Now let’s use what we’ve learned about FLATTEN to convert all of the
--         data to a structured format.

-- 6.3.7   Explode the data
--         Now we want to list ID, first name, last name, business phone number
--         and email, and personal phone number and email. We need to explode
--         the data to do that. But first we need to figure out the input into
--         our FLATTEN function.
--         Start by running the query below:

SELECT
   info.*
FROM
   customers,
LATERAL FLATTEN(input=>info, RECURSIVE=>true) info
WHERE index IS NOT NULL;

--         Using what we learned earlier, we exploded all of the data but
--         filtered to only find the rows where index is not null. This gives us
--         all of the positions in the contact array.
--         If we look in the path column, we find contact[0] and contact[1].
--         Those are the values we want to input into our FLATTEN function.

-- 6.3.8   Fetch ID, first name, last name and business details
--         Run the statement below:

SELECT
   ID,
   info:name.first::VARCHAR AS first_name,
   info:name.last::VARCHAR AS last_name,
   business.value:phone::VARCHAR as business_phone,
   business.value:email::VARCHAR as business_email
FROM
   customers,
LATERAL FLATTEN(input=>info:contact[0]) business;

--         Fetching the ID, first and last names was simple. For the business
--         details, we’ve inputted contact[0] into the FLATTEN function to
--         access only the business details.

-- 6.3.9   Fetch ID, first name, last name and personal details
--         Run the statement below:

SELECT
   ID,
   info:name.first::VARCHAR AS first_name,
   info:name.last::VARCHAR AS last_name,
   personal.value:phone::VARCHAR as personal_phone,
   personal.value:email::VARCHAR as personal_email
FROM
   customers,
LATERAL FLATTEN(input=>info:contact[1]) personal;

--         Here we’ve fetched the personal details exactly as we fetched the
--         business details. Now let’s put it all together.

-- 6.3.10  Fetch all contact details

SELECT
   ID,
   info:name.first::VARCHAR AS first_name,
   info:name.last::VARCHAR AS last_name,
   business.value:email::VARCHAR as business_email,
   business.value:phone::VARCHAR as business_phone,
   personal.value:email::VARCHAR as personal_email,
   personal.value:phone::VARCHAR as personal_phone
FROM
   customers,
LATERAL FLATTEN(input=>info:contact[0]) business,
LATERAL FLATTEN(input=>info:contact[1]) personal;

--         Here we simply used the FLATTEN function twice for each potential
--         path. Our semi-structured data is now structured data.

-- 6.4.0   Try it out!
--         In this section, you’re going to query semi-structured data on your
--         own. First you’ll run a statement to create a short set of JSON data,
--         then we’ll walk you through some challenge exercises.

-- 6.4.1   Setting the context
--         Set the context using the code shown below:

CREATE WAREHOUSE IF NOT EXISTS GATOR_WH;
USE WAREHOUSE GATOR_WH;

CREATE DATABASE IF NOT EXISTS GATOR_DB;
USE DATABASE GATOR_DB;

CREATE OR REPLACE SCHEMA WEATHER;
USE SCHEMA WEATHER;


-- 6.4.2   Run the statement below to create the data you’ll query

CREATE OR REPLACE TABLE weather_data 
AS 
SELECT
   parse_json($1) AS w
FROM VALUES
('{
  "data": {
    "observations": [
      {
        "air": {
          "dew-point": 8.2,
          "dew-point-quality-code": "1",
          "temp": 29.8,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10161,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T02:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 80,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 41
        }
      },
      {
        "air": {
          "dew-point": 8.2,
          "dew-point-quality-code": "1",
          "temp": 29.9,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10161,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T02:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 80,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 41
        }
      },
      {
        "air": {
          "dew-point": 6.4,
          "dew-point-quality-code": "1",
          "temp": 32.2,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10126,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T05:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 60,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 57
        }
      },
      {
        "air": {
          "dew-point": 6.4,
          "dew-point-quality-code": "1",
          "temp": 32.2,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10126,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T05:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 60,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 57
        }
      },
      {
        "air": {
          "dew-point": 5.1,
          "dew-point-quality-code": "1",
          "temp": 30.6,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10123,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T08:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 140,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 26
        }
      },
      {
        "air": {
          "dew-point": 5.1,
          "dew-point-quality-code": "1",
          "temp": 30.7,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10123,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T08:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 140,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 26
        }
      },
      {
        "air": {
          "dew-point": 3.5,
          "dew-point-quality-code": "1",
          "temp": 18.9,
          "temp-quality-code": "2"
        },
        "atmospheric": {
          "pressure": 10152,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T11:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 120,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 10
        }
      },
      {
        "air": {
          "dew-point": 3.5,
          "dew-point-quality-code": "1",
          "temp": 19,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10152,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T11:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 120,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 10
        }
      },
      {
        "air": {
          "dew-point": 5.5,
          "dew-point-quality-code": "1",
          "temp": 17.9,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10165,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T14:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 150,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 10
        }
      },
      {
        "air": {
          "dew-point": 5.5,
          "dew-point-quality-code": "1",
          "temp": 18,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10165,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T14:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 150,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 10
        }
      },
      {
        "air": {
          "dew-point": 5,
          "dew-point-quality-code": "1",
          "temp": 13.3,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10163,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T17:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 140,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 5
        }
      },
      {
        "air": {
          "dew-point": 5,
          "dew-point-quality-code": "1",
          "temp": 13.4,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10163,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T17:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 140,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 5
        }
      },
      {
        "air": {
          "dew-point": 7.8,
          "dew-point-quality-code": "1",
          "temp": 13.8,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10177,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T20:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 160,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 15
        }
      },
      {
        "air": {
          "dew-point": 7.8,
          "dew-point-quality-code": "1",
          "temp": 13.9,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10177,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T20:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 160,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 15
        }
      },
      {
        "air": {
          "dew-point": 9.4,
          "dew-point-quality-code": "1",
          "temp": 22.2,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10190,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T23:00:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 130,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 21
        }
      },
      {
        "air": {
          "dew-point": 9.4,
          "dew-point-quality-code": "1",
          "temp": 22.2,
          "temp-quality-code": "1"
        },
        "atmospheric": {
          "pressure": 10190,
          "pressure-quality-code": "1"
        },
        "dt": "2019-07-30T23:30:00",
        "sky": {
          "ceiling": 99999,
          "ceiling-quality-code": "9"
        },
        "visibility": {
          "distance": 999999,
          "distance-quality-code": "9"
        },
        "wind": {
          "direction-angle": 130,
          "direction-quality-code": "1",
          "speed-quality-code": "1",
          "speed-rate": 21
        }
      }
    ]
  },
  "station": {
    "USAF": "942340",
    "WBAN": 99999,
    "coord": {
      "lat": -16.25,
      "lon": 133.367
    },
    "country": "AS",
    "elev": 211,
    "id": "94234099999",
    "name": "DALY WATERS AWS"
  }
}');


-- 6.4.3   Take a look at the following diagram to better understand the data
--         you just created
--         In the previous step, you created a single document that is
--         structured as shown in the diagram. As you can see, there are two
--         top-level keys in the data: data and station.
--         Data (the first key) has a corresponding value that is an object.
--         That object contains an array called observations. This array
--         contains objects, each of which has six elements: a key-value pair
--         called dt, and five key-value pairs named air, atmospheric, sky,
--         visibility, and wind. With the exception of dt (which is a simple
--         key-value pair), the value in each of the key-value pairs is an
--         object that in turn contains a set of simple key-value pairs.
--         Station (the second key) has a corresponding value that in an object.
--         That object consists of six key-value pairs and an object called
--         coord. This object consists of two key-value pairs: lat and lon.
--         The general idea is that for a specific date at a specific station, a
--         number of weather measurements (or observations) are taken. In this
--         case, you have created a set of sixteen observations taken at
--         different times throughout the day on Tuesday, July 30, 2019.
--         Now you’ll be given a set of challenges based on what you’ve just
--         learned. Working with semi-structured data can be tricky, and don’t
--         worry if you don’t answer each challenge correctly. The point here is
--         to try to apply the points in this lesson and to learn as much as you
--         can. There is a solution at the end of this exercise if you need it.

-- 6.4.4   Select USAF, WBAN, country, elev, id and name from station using dot
--         notation.

-- 6.4.5   Select average, max and min air temperature using LATERAL FLATTEN.

-- 6.4.6   Select max and min atmospheric pressure using LATERAL FLATTEN.

-- 6.4.7   Use either dot notation or FLATTEN LATERAL to fetch the dew point,
--         atmospheric pressure, and wind speed rate for the 15th observation.

-- 6.4.8   Check the solution just after the Key Takeaways if you need help.
--         Otherwise congratulations! You’ve just completed this lab.

-- 6.5.0   Key Takeaways
--         - JSON data, is stored in a column of data type VARIANT. This is true
--         for all other forms of semi-structured data capable of being stored
--         in Snowflake.
--         - While you can use VARIANT data types in comparisons or aggregations
--         with other data types, your queries won’t be as performant as they
--         would be if you had cast the VARIANT columns to standard SQL data
--         types.
--         - You can use either dot notation or LATERAL FLATTEN to query semi-
--         structured data in an array.
--         - When working with arrays in nested JSON data, you will need to
--         provide the index of the key-value pair in the array that you want so
--         you can access its contents.
--         - You can use the FLATTEN function’s output fields seq, key, path,
--         index, value and this to figure out how your semi-structured data is
--         structured.
--         - You can use FLATTEN multiple times in a FROM clause for each
--         potential path.

-- 6.6.0   Solution

--select USAF, WBAN, country, elev, id and name from station using dot notation
SELECT 
        wd.w:station.USAF::VARCHAR AS USAF, 
        wd.w:station.country::VARCHAR AS country,
        wd.w:station.elev::VARCHAR AS elev,
        wd.w:station.id::VARCHAR AS id,
        wd.w:station.name::VARCHAR AS name
 FROM 
    weather_data wd;
    

-- select average, max and min air temperature using LATERAL FLATTEN 
SELECT 
          AVG(f.value:air.temp)::NUMBER(38,1) as avg_temp_c,
          MIN(f.value:air.temp) as min_temp_c,
          MAX(f.value:air.temp) as max_temp_c

FROM 
    weather_data wd, 
    LATERAL FLATTEN (input => w:data.observations) f  ;  


-- select max and min atmospheric pressure using LATERAL FLATTEN
SELECT 
          MAX(f.value:atmospheric.pressure)::NUMBER(38,1) as max_pressure,
          MIN(f.value:atmospheric.pressure)::NUMBER(38,1) as min_pressure
FROM 
    weather_data wd, 
    LATERAL FLATTEN (input => w:data.observations) f  ;


-- Use either dot notation or FLATTEN LATERAL to fetch the dew point, atmospheric pressure, and wind speed rate for the 15th observation
--dot notation
 SELECT 
        wd.w:data.observations[15].air."dew-point"::NUMBER(38,1) AS dew_point,
        wd.w:data.observations[15].atmospheric.pressure::NUMBER(38,1) AS pressure,
        wd.w:data.observations[15].wind."speed-rate"::NUMBER(38,1) AS dew_point
        
 FROM 
    weather_data wd;
    
-- flatten lateral
SELECT
        datapoint1.VALUE::NUMBER(38,1) AS dew_point,
        datapoint2.VALUE::NUMBER(38,1) AS pressure,
        datapoint3.VALUE::NUMBER(38,1) AS speed_rate
FROM 
    weather_data wd, 
    LATERAL FLATTEN (input => w:data.observations[15].air."dew-point", recursive => true) datapoint1,
    LATERAL FLATTEN (input => w:data.observations[15].atmospheric.pressure, recursive => true) datapoint2,
    LATERAL FLATTEN (input => w:data.observations[15].wind."speed-rate", recursive => true) datapoint3;

