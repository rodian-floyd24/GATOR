-- Query 1: Multi-table JOIN across trades, bonds, issuers, purposes
-- Returns top 20 bonds by traded quantity with issuer and purpose context.
SELECT
  b.bond_id,
  i.name        AS issuer_name,
  i.state_code  AS state,
  COALESCE(p.code, 'UNSPEC') AS purpose_category,
  ROUND(AVG(t.price), 2)     AS avg_trade_price,
  SUM(t.quantity)            AS total_quantity
FROM trades t
JOIN bonds b          ON t.bond_id  = b.bond_id
JOIN issuers i        ON b.issuer_id = i.issuer_id
LEFT JOIN bond_purposes p ON b.purpose_id = p.purpose_id
GROUP BY b.bond_id, issuer_name, state, purpose_category
ORDER BY total_quantity DESC, avg_trade_price DESC
LIMIT 20;

-- Query 2: Aggregation with GROUP BY (state, purpose) and HAVING clause
-- Highlights state-purpose segments with at least 500 bonds traded.
SELECT
  i.state_code                       AS state,
  COALESCE(p.code, 'UNSPEC')         AS purpose_category,
  COUNT(DISTINCT b.bond_id)          AS bonds_traded,
  SUM(t.quantity)                    AS total_quantity,
  ROUND(AVG(t.price), 2)             AS avg_trade_price
FROM trades t
JOIN bonds b          ON t.bond_id  = b.bond_id
JOIN issuers i        ON b.issuer_id = i.issuer_id
LEFT JOIN bond_purposes p ON b.purpose_id = p.purpose_id
GROUP BY state, purpose_category
HAVING SUM(t.quantity) >= 500
ORDER BY total_quantity DESC
LIMIT 20;

-- Query 3: Window-based comparison of first vs latest bond rating
-- Returns bonds whose rating changed over time.
WITH ranked AS (
  SELECT
    b.bond_id,
    i.name AS issuer_name,
    cr.rating_code,
    cr.rating_date,
    ROW_NUMBER() OVER (PARTITION BY b.bond_id ORDER BY cr.rating_date DESC) AS rn_desc,
    ROW_NUMBER() OVER (PARTITION BY b.bond_id ORDER BY cr.rating_date ASC)  AS rn_asc
  FROM bonds b
  JOIN issuers i        ON b.issuer_id = i.issuer_id
  JOIN credit_ratings cr ON cr.bond_id = b.bond_id
)
SELECT
  bond_id,
  issuer_name,
  MAX(CASE WHEN rn_desc = 1 THEN rating_code END) AS latest_rating,
  MAX(CASE WHEN rn_asc  = 1 THEN rating_code END) AS first_rating,
  MAX(CASE WHEN rn_desc = 1 THEN rating_date END) AS latest_rating_date
FROM ranked
GROUP BY bond_id, issuer_name
HAVING latest_rating <> first_rating
LIMIT 20;

-- Query 4: Date-based trend analysis using TO_CHAR for monthly roll-ups
-- Summarizes trade activity by calendar month.
SELECT
  TO_CHAR(trade_date, 'YYYY-MM') AS trade_month,
  COUNT(*)                       AS trades_count,
  SUM(quantity)                  AS total_quantity,
  ROUND(AVG(price), 2)           AS avg_trade_price
FROM trades
GROUP BY trade_month
ORDER BY trade_month
LIMIT 20;

-- Query 5: Financial metric calculation (coupon spread vs 10Y Treasury)
-- Calculates average municipal coupon spread per state-month.
WITH ten_yr AS (
  SELECT
    geo_code,
    DATE_TRUNC('MONTH', period_start_date) AS period_month,
    value AS treasury_10yr
  FROM economic_indicators
  WHERE indicator_name = 'TREASURY_10YR'
)
SELECT
  i.state_code                               AS state,
  TO_CHAR(t.trade_date, 'YYYY-MM')           AS trade_month,
  ROUND(AVG(b.coupon_rate), 2)               AS avg_coupon_rate,
  ROUND(AVG(ten_yr.treasury_10yr), 2)        AS avg_treasury_10yr,
  ROUND(AVG(b.coupon_rate - ten_yr.treasury_10yr), 2) AS avg_coupon_spread
FROM trades t
JOIN bonds b   ON t.bond_id = b.bond_id
JOIN issuers i ON b.issuer_id = i.issuer_id
JOIN ten_yr    ON ten_yr.geo_code = i.state_code
              AND ten_yr.period_month = DATE_TRUNC('MONTH', t.trade_date)
WHERE b.coupon_rate IS NOT NULL
GROUP BY state, trade_month
HAVING COUNT(*) >= 10
ORDER BY avg_coupon_spread DESC
LIMIT 20;
