import streamlit as st
from snowflake.snowpark.context import get_active_session

st.title("Muni Bonds – Week 11 Queries")
st.caption("Runs the five Snowflake queries from the Week 11 analysis against GATOR_DB.MUNI.")

session = get_active_session()

QUERIES = [
    (
        "Query 1 – Top traded bonds with issuer & purpose",
        """
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
        """,
    ),
    (
        "Query 2 – State–purpose hotspots",
        """
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
        """,
    ),
    (
        "Query 3 – Rating migration monitor",
        """
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
        """,
    ),
    (
        "Query 4 – Monthly trade trendline",
        """
        SELECT
          TO_CHAR(trade_date, 'YYYY-MM') AS trade_month,
          COUNT(*)                       AS trades_count,
          SUM(quantity)                  AS total_quantity,
          ROUND(AVG(price), 2)           AS avg_trade_price
        FROM trades
        GROUP BY trade_month
        ORDER BY trade_month
        LIMIT 20;
        """,
    ),
    (
        "Query 5 – Coupon spread vs 10Y Treasury",
        """
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
        """,
    ),
]


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str):
    return session.sql(sql).to_pandas()


tabs = st.tabs([q[0] for q in QUERIES])

for tab, (title, sql) in zip(tabs, QUERIES):
    with tab:
        st.subheader(title)
        st.code(sql.strip(), language="sql")
        try:
            df = run_query(sql)
            st.dataframe(df, use_container_width=True)
        except Exception as exc:
            st.error("Query failed. Check database/schema or privileges.")
            st.exception(exc)
