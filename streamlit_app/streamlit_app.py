import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import date

st.title("Muni Bonds – Week 11 Queries")
st.caption("Runs the five Snowflake queries from the Week 11 analysis against GATOR_DB.MUNI with interactive filters.")

session = get_active_session()

@st.cache_data(ttl=600, show_spinner=False)
def get_meta():
    date_df = session.sql(
        "SELECT MIN(trade_date) AS min_date, MAX(trade_date) AS max_date FROM trades"
    ).to_pandas()
    min_date = date_df["MIN_DATE"].iloc[0]
    max_date = date_df["MAX_DATE"].iloc[0]
    # Fallback in case table is empty
    if pd.isna(min_date) or pd.isna(max_date):
        min_date = max_date = date.today()
    min_date = min_date.date() if hasattr(min_date, "date") else min_date
    max_date = max_date.date() if hasattr(max_date, "date") else max_date

    states_df = session.sql(
        "SELECT DISTINCT state_code FROM issuers WHERE state_code IS NOT NULL ORDER BY state_code"
    ).to_pandas()
    states = states_df["STATE_CODE"].dropna().tolist()
    return min_date, max_date, states


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str):
    return session.sql(sql).to_pandas()


min_date, max_date, state_options = get_meta()

st.sidebar.header("Filters")
date_range = st.sidebar.date_input(
    "Trade date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    st.stop()

selected_states = st.sidebar.multiselect(
    "States",
    options=state_options,
    default=state_options,
    help="Filter issuer states (leave all selected for no state filter).",
)

row_limit = st.sidebar.slider("Row limit", min_value=10, max_value=200, value=50, step=10)


def build_state_filter(states):
    if not states or len(states) == len(state_options):
        return ""
    quoted = ", ".join(f"'{s}'" for s in states)
    return f" AND i.state_code IN ({quoted})"


def q1(states, start, end, limit):
    state_filter = build_state_filter(states)
    return f"""
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
    WHERE t.trade_date BETWEEN '{start}' AND '{end}'{state_filter}
    GROUP BY b.bond_id, issuer_name, state, purpose_category
    ORDER BY total_quantity DESC, avg_trade_price DESC
    LIMIT {limit};
    """


def q2(states, start, end, limit):
    state_filter = build_state_filter(states)
    return f"""
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
    WHERE t.trade_date BETWEEN '{start}' AND '{end}'{state_filter}
    GROUP BY state, purpose_category
    HAVING SUM(t.quantity) >= 100
    ORDER BY total_quantity DESC
    LIMIT {limit};
    """


def q3(states, limit):
    state_filter = build_state_filter(states)
    return f"""
    WITH ranked AS (
      SELECT
        b.bond_id,
        i.name AS issuer_name,
        i.state_code AS state,
        cr.rating_code,
        cr.rating_date,
        ROW_NUMBER() OVER (PARTITION BY b.bond_id ORDER BY cr.rating_date DESC) AS rn_desc,
        ROW_NUMBER() OVER (PARTITION BY b.bond_id ORDER BY cr.rating_date ASC)  AS rn_asc
      FROM bonds b
      JOIN issuers i        ON b.issuer_id = i.issuer_id
      JOIN credit_ratings cr ON cr.bond_id = b.bond_id
      WHERE 1=1{state_filter}
    )
    SELECT
      bond_id,
      issuer_name,
      state,
      MAX(CASE WHEN rn_desc = 1 THEN rating_code END) AS latest_rating,
      MAX(CASE WHEN rn_asc  = 1 THEN rating_code END) AS first_rating,
      MAX(CASE WHEN rn_desc = 1 THEN rating_date END) AS latest_rating_date
    FROM ranked
    GROUP BY bond_id, issuer_name, state
    HAVING latest_rating <> first_rating
    LIMIT {limit};
    """


def q4(states, start, end, limit):
    state_filter = build_state_filter(states)
    return f"""
    SELECT
      TO_CHAR(t.trade_date, 'YYYY-MM') AS trade_month,
      COUNT(*)                         AS trades_count,
      SUM(t.quantity)                  AS total_quantity,
      ROUND(AVG(t.price), 2)           AS avg_trade_price
    FROM trades t
    JOIN bonds b ON t.bond_id = b.bond_id
    JOIN issuers i ON b.issuer_id = i.issuer_id
    WHERE t.trade_date BETWEEN '{start}' AND '{end}'{state_filter}
    GROUP BY trade_month
    ORDER BY trade_month
    LIMIT {limit};
    """


def q5(states, start, end, limit):
    state_filter = build_state_filter(states)
    return f"""
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
      AND t.trade_date BETWEEN '{start}' AND '{end}'{state_filter}
    GROUP BY state, trade_month
    HAVING COUNT(*) >= 10
    ORDER BY avg_coupon_spread DESC
    LIMIT {limit};
    """


QUERIES = [
    ("Query 1 – Top traded bonds with issuer & purpose", lambda: q1(selected_states, start_date, end_date, row_limit)),
    ("Query 2 – State–purpose hotspots", lambda: q2(selected_states, start_date, end_date, row_limit)),
    ("Query 3 – Rating migration monitor", lambda: q3(selected_states, row_limit)),
    ("Query 4 – Monthly trade trendline", lambda: q4(selected_states, start_date, end_date, row_limit)),
    ("Query 5 – Coupon spread vs 10Y Treasury", lambda: q5(selected_states, start_date, end_date, row_limit)),
]


tabs = st.tabs([q[0] for q in QUERIES])

for tab, (title, builder) in zip(tabs, QUERIES):
    with tab:
        st.subheader(title)
        sql = builder()
        st.code(sql.strip(), language="sql")
        try:
            df = run_query(sql)
            if df.empty:
                st.info("No rows returned for the current filters.")
            else:
                st.dataframe(df, use_container_width=True)
                df_plot = df.copy()
                df_plot.columns = [c.lower() for c in df_plot.columns]

                # ---------- PLOTS PER QUERY ----------
                if title.startswith("Query 1"):
                    chart = (
                        alt.Chart(df_plot)
                        .mark_bar()
                        .encode(
                            x=alt.X("bond_id:N", sort="-y", title="Bond ID"),
                            y=alt.Y("total_quantity:Q", title="Total Quantity Traded"),
                            tooltip=[
                                "bond_id",
                                "issuer_name",
                                "state",
                                "purpose_category",
                                "avg_trade_price",
                                "total_quantity",
                            ],
                        )
                        .properties(height=400)
                    )
                    st.altair_chart(chart, use_container_width=True)

                elif title.startswith("Query 2"):
                    chart = (
                        alt.Chart(df_plot)
                        .mark_rect()
                        .encode(
                            x=alt.X("state:N", title="State"),
                            y=alt.Y("purpose_category:N", title="Purpose"),
                            color=alt.Color("total_quantity:Q", title="Total Quantity"),
                            tooltip=[
                                "state",
                                "purpose_category",
                                "bonds_traded",
                                "total_quantity",
                                "avg_trade_price",
                            ],
                        )
                        .properties(height=400)
                    )
                    st.altair_chart(chart, use_container_width=True)

                elif title.startswith("Query 3"):
                    df_q3 = df_plot.copy()
                    df_q3["migration"] = df_q3["first_rating"] + " → " + df_q3["latest_rating"]
                    counts = df_q3["migration"].value_counts().reset_index()
                    counts.columns = ["migration", "n_bonds"]

                    chart = (
                        alt.Chart(counts)
                        .mark_bar()
                        .encode(
                            x=alt.X("migration:N", sort="-y", title="Rating Migration"),
                            y=alt.Y("n_bonds:Q", title="Number of Bonds"),
                            tooltip=["migration", "n_bonds"],
                        )
                        .properties(height=400)
                    )
                    st.altair_chart(chart, use_container_width=True)

                elif title.startswith("Query 4"):
                    chart = (
                        alt.Chart(df_plot)
                        .mark_line(point=True)
                        .encode(
                            x=alt.X("trade_month:N", title="Month"),
                            y=alt.Y("total_quantity:Q", title="Total Quantity Traded"),
                            tooltip=["trade_month", "trades_count", "total_quantity", "avg_trade_price"],
                        )
                        .properties(height=400)
                    )
                    st.altair_chart(chart, use_container_width=True)

                elif title.startswith("Query 5"):
                    chart = (
                        alt.Chart(df_plot)
                        .mark_line(point=True)
                        .encode(
                            x=alt.X("trade_month:N", title="Month"),
                            y=alt.Y("avg_coupon_spread:Q", title="Avg Coupon – 10Y Treasury"),
                            color=alt.Color("state:N", title="State"),
                            tooltip=[
                                "state",
                                "trade_month",
                                "avg_coupon_rate",
                                "avg_treasury_10yr",
                                "avg_coupon_spread",
                            ],
                        )
                        .properties(height=400)
                    )
                    st.altair_chart(chart, use_container_width=True)
                # ---------- END PLOTS ----------

                st.download_button(
                    "Download CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{title.replace(' ', '_').lower()}.csv",
                    mime="text/csv",
                )
        except Exception as exc:
            st.error("Query failed. Check database/schema or privileges.")
            st.exception(exc)
