-- Exploring Semi-Structured JSON Data (personalized, idempotent)
-- Uses: ROLE TRAINING_ROLE, WH GATOR_WH, DB GATOR_DB (schemas: GATOR_SCHEMA, WEATHER)

-- 6.1.0 Setup context
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS GATOR_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE GATOR_WH SET WAREHOUSE_SIZE = 'XSMALL';
USE WAREHOUSE GATOR_WH;

CREATE DATABASE IF NOT EXISTS GATOR_DB;
CREATE SCHEMA IF NOT EXISTS GATOR_DB.GATOR_SCHEMA;
USE SCHEMA GATOR_DB.GATOR_SCHEMA;

-- 6.1.2 Simple JSON table with VARIANT and dot notation
CREATE OR REPLACE TABLE customers AS
SELECT
  $1 AS id,
  parse_json($2) AS info
FROM VALUES
  (12712555, '{"name": {"first": "John", "last":"Smith"}}'),
  (98127771, '{"name": {"first": "Jane", "last":"Doe"}}');

SELECT * FROM customers;

-- Extract values with dot notation (still VARIANT)
SELECT
  id,
  info:name.first AS first_name,
  info:name.last AS last_name
FROM customers;

-- Inspect result metadata
DESCRIBE RESULT last_query_id();

-- Cast VARIANT to VARCHAR for performance and usability
SELECT
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name
FROM customers;

DESCRIBE RESULT last_query_id();

-- Persist as structured table
CREATE OR REPLACE TABLE customers_structured AS
SELECT 
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name
FROM customers;

SELECT * FROM customers_structured;

-- 6.2.0 Nested JSON + arrays; dot notation with indexes
CREATE OR REPLACE TABLE customers AS 
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
      ]
    }'
  ),
  (
    98127771,
    '{"name": {"first":"Jane", "last":"Doe"},
      "contact": [
        {"business": {"phone":"303-638-4887", "email":"jg_doe@company2.com"}},
        {"personal": {"phone":"303-678-6789", "email":"happyjane@gmail.com"}}
      ]
    }'
  );

SELECT * FROM customers;

-- Array access without indexes yields NULLs for nested fields
SELECT
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name,
  info:contact.business.phone::VARCHAR AS business_phone,
  info:contact.personal.phone::VARCHAR AS personal_phone
FROM customers;

-- Use array indexes to select nested values
SELECT
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name,
  info:contact[0].business.phone::VARCHAR AS business_phone,
  info:contact[1].personal.phone::VARCHAR AS personal_phone
FROM customers;

-- Persist structured result from nested JSON
CREATE OR REPLACE TABLE customers_dot AS
SELECT
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name,
  info:contact[0].business.phone::VARCHAR AS business_phone,
  info:contact[0].business.email::VARCHAR AS business_email,
  info:contact[1].personal.phone::VARCHAR AS personal_phone,
  info:contact[1].personal.email::VARCHAR AS personal_email
FROM customers;

SELECT * FROM customers_dot;

-- 6.3.0 FLATTEN for inspection and extraction
-- Top-level keys
SELECT *
FROM customers,
LATERAL FLATTEN(input => info);

SELECT key, value
FROM customers,
LATERAL FLATTEN(input => info);

-- Recursive flatten reveals nested keys and positions
SELECT key, index, value
FROM customers,
LATERAL FLATTEN(input => info, RECURSIVE => true);

-- Only simple values (exclude objects/arrays)
SELECT key, index, value
FROM customers,
LATERAL FLATTEN(input => info, RECURSIVE => true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

SELECT key, value
FROM customers,
LATERAL FLATTEN(input => info, RECURSIVE => true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

-- Include helper columns
SELECT key, value, this
FROM customers,
LATERAL FLATTEN(input => info, RECURSIVE => true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

SELECT key, value, path
FROM customers,
LATERAL FLATTEN(input => info, RECURSIVE => true)
WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY');

SELECT seq, key, value
FROM customers,
LATERAL FLATTEN(input => info, RECURSIVE => true)
WHERE IS_ARRAY(value) = 'false' AND IS_OBJECT(value) = 'false'
ORDER BY seq;

-- Explode contacts to identify inputs for flatten
SELECT info.*
FROM customers,
LATERAL FLATTEN(input => info, RECURSIVE => true) info
WHERE index IS NOT NULL;

-- Business details via FLATTEN
SELECT
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name,
  business.value:phone::VARCHAR AS business_phone,
  business.value:email::VARCHAR AS business_email
FROM customers,
LATERAL FLATTEN(input => info:contact[0]) business;

-- Personal details via FLATTEN
SELECT
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name,
  personal.value:phone::VARCHAR AS personal_phone,
  personal.value:email::VARCHAR AS personal_email
FROM customers,
LATERAL FLATTEN(input => info:contact[1]) personal;

-- Combine both via two FLATTENs
SELECT
  id,
  info:name.first::VARCHAR AS first_name,
  info:name.last::VARCHAR AS last_name,
  business.value:email::VARCHAR AS business_email,
  business.value:phone::VARCHAR AS business_phone,
  personal.value:email::VARCHAR AS personal_email,
  personal.value:phone::VARCHAR AS personal_phone
FROM customers,
LATERAL FLATTEN(input => info:contact[0]) business,
LATERAL FLATTEN(input => info:contact[1]) personal;

-- 6.4.0 Try it out: Weather dataset in separate schema
CREATE SCHEMA IF NOT EXISTS GATOR_DB.WEATHER;
USE SCHEMA GATOR_DB.WEATHER;

-- Create a single-row table containing a complex JSON document
CREATE OR REPLACE TABLE weather_data AS
SELECT parse_json($1) AS w
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
    "coord": {"lat": -16.25, "lon": 133.367},
    "country": "AS",
    "elev": 211,
    "id": "94234099999",
    "name": "DALY WATERS AWS"
  }
}');

-- 6.4.3–6.4.7 Exercises (refer to solution below if needed)
-- 6.4.4 Dot notation over station
-- 6.4.5/6 Aggregate temps and pressures with LATERAL FLATTEN
-- 6.4.7 15th observation fields via dot or FLATTEN

-- 6.6.0 Solution examples
-- Station attributes
SELECT 
  wd.w:station.USAF::VARCHAR AS USAF,
  wd.w:station.WBAN::VARCHAR AS WBAN,
  wd.w:station.country::VARCHAR AS country,
  wd.w:station.elev::VARCHAR AS elev,
  wd.w:station.id::VARCHAR AS id,
  wd.w:station.name::VARCHAR AS name
FROM weather_data wd;

-- Average, min, max air temperature
SELECT 
  AVG(f.value:air.temp)::NUMBER(38,1) AS avg_temp_c,
  MIN(f.value:air.temp) AS min_temp_c,
  MAX(f.value:air.temp) AS max_temp_c
FROM weather_data wd,
LATERAL FLATTEN(input => w:data.observations) f;

-- Max/min atmospheric pressure
SELECT 
  MAX(f.value:atmospheric.pressure)::NUMBER(38,1) AS max_pressure,
  MIN(f.value:atmospheric.pressure)::NUMBER(38,1) AS min_pressure
FROM weather_data wd,
LATERAL FLATTEN(input => w:data.observations) f;

-- 15th observation via dot notation
SELECT 
  wd.w:data.observations[15].air."dew-point"::NUMBER(38,1) AS dew_point,
  wd.w:data.observations[15].atmospheric.pressure::NUMBER(38,1) AS pressure,
  wd.w:data.observations[15].wind."speed-rate"::NUMBER(38,1) AS speed_rate
FROM weather_data wd;

-- 15th observation via FLATTEN
SELECT
  datapoint1.VALUE::NUMBER(38,1) AS dew_point,
  datapoint2.VALUE::NUMBER(38,1) AS pressure,
  datapoint3.VALUE::NUMBER(38,1) AS speed_rate
FROM weather_data wd,
LATERAL FLATTEN(input => w:data.observations[15].air."dew-point", RECURSIVE => true) datapoint1,
LATERAL FLATTEN(input => w:data.observations[15].atmospheric.pressure, RECURSIVE => true) datapoint2,
LATERAL FLATTEN(input => w:data.observations[15].wind."speed-rate", RECURSIVE => true) datapoint3;

