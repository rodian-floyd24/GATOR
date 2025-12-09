import streamlit as st
import pandas as pd
import altair as alt
from datetime import date
from pathlib import Path

try:
    from snowflake.snowpark.context import get_active_session
    from snowflake.snowpark.exceptions import SnowparkSessionException
except ImportError:
    get_active_session = None
    SnowparkSessionException = Exception

st.title("Muni Bonds – Week 11 Queries")
st.caption("Runs the five Snowflake queries from the Week 11 analysis against GATOR_DB.MUNI with interactive filters.")

session = None
if get_active_session:
    try:
        session = get_active_session()
    except SnowparkSessionException:
        session = None
    except Exception:
        session = None
BASE_DIR = Path(".")

QUERY_SUMMARIES = {
    "Query 1 – Top traded bonds with issuer & purpose": {
        "purpose": "Identify the most actively traded bonds and show their issuers, purposes, and pricing.",
        "tables": "trades → bonds → issuers (issuer names/states) + bond_purposes for category labels.",
        "filters": "Trade date range, optional state filter, row limit; quantity drives the ordering.",
    },
    "Query 2 – State–purpose hotspots": {
        "purpose": "Spot combinations of state and bond purpose that concentrate trading activity.",
        "tables": "trades → bonds → issuers (states) + bond_purposes; aggregates by state/purpose.",
        "filters": "Trade date range, optional state filter, row limit; ignores cells with <100 quantity.",
    },
    "Query 3 – Rating migration monitor": {
        "purpose": "Flag bonds whose credit rating changed over time.",
        "tables": "bonds → issuers + credit_ratings (first vs latest rating using window functions).",
        "filters": "Optional state filter, row limit; returns only bonds where first and latest ratings differ.",
    },
    "Query 4 – Monthly trade trendline": {
        "purpose": "Track trading intensity by month.",
        "tables": "trades → bonds → issuers; aggregates trades, quantity, and prices by month.",
        "filters": "Trade date range, optional state filter, row limit; ordered chronologically.",
    },
    "Query 5 – Coupon spread vs 10Y Treasury": {
        "purpose": "Compare muni coupon rates to the 10Y Treasury by state/month to see spread patterns.",
        "tables": "trades → bonds (coupon_rate) → issuers joined to economic_indicators (TREASURY_10YR).",
        "filters": "Trade date range, optional state filter, row limit; keeps months with ≥10 trades.",
    },
}


@st.cache_data(ttl=600, show_spinner=False)
def get_meta():
    if session:
        date_df = session.sql(
            "SELECT MIN(trade_date) AS min_date, MAX(trade_date) AS max_date FROM trades"
        ).to_pandas()
        min_date = date_df["MIN_DATE"].iloc[0]
        max_date = date_df["MAX_DATE"].iloc[0]
        if pd.isna(min_date) or pd.isna(max_date):
            min_date = max_date = date.today()
        min_date = min_date.date() if hasattr(min_date, "date") else min_date
        max_date = max_date.date() if hasattr(max_date, "date") else max_date

        states_df = session.sql(
            "SELECT DISTINCT state_code FROM issuers WHERE state_code IS NOT NULL ORDER BY state_code"
        ).to_pandas()
        states = states_df["STATE_CODE"].dropna().tolist()
        return min_date, max_date, states
    # Fallback to local data if Snowflake/Snowpark is unavailable
    df_local, _, _, _ = load_visual_data()
    min_date = df_local["trade_date"].min().date()
    max_date = df_local["trade_date"].max().date()
    states = sorted(df_local["state_code"].dropna().unique().tolist())
    return min_date, max_date, states


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str):
    if not session:
        raise RuntimeError("Snowflake session not available. Install snowflake-snowpark-python or run in Snowsight.")
    return session.sql(sql).to_pandas()


# ---------- DATA LAYER FOR VISUALIZATIONS ----------
@st.cache_data(ttl=600, show_spinner=False)
def load_visual_data():
    """
    Load from Snowflake tables when a session is available; otherwise fall back to local CSVs.
    """
    if session:
        bonds = session.table("GATOR_DB.MUNI.BONDS").to_pandas()
        trades = session.table("GATOR_DB.MUNI.TRADES").to_pandas()
        issuers = session.table("GATOR_DB.MUNI.ISSUERS").to_pandas()
        purposes = session.table("GATOR_DB.MUNI.BOND_PURPOSES").to_pandas()
        ratings = session.table("GATOR_DB.MUNI.CREDIT_RATINGS").to_pandas()
        econ = session.table("GATOR_DB.MUNI.ECONOMIC_INDICATORS").to_pandas()
        source = "Snowflake tables (GATOR_DB.MUNI)"
    else:
        bonds = pd.read_csv(BASE_DIR / "bonds.csv", parse_dates=["issue_date", "maturity_date"])
        trades = pd.read_csv(BASE_DIR / "trades.csv", parse_dates=["trade_date"]).rename(
            columns={"price": "trade_price", "yield_pct": "yield"}
        )
        issuers = pd.read_csv(BASE_DIR / "issuers.csv")
        purposes = pd.read_csv(BASE_DIR / "bond_purposes.csv")
        ratings = pd.read_csv(BASE_DIR / "credit_ratings.csv", parse_dates=["rating_date"])
        econ = pd.read_csv(BASE_DIR / "economic_indicators.csv", parse_dates=["date"])
        source = "Local CSVs"

    if "state_code" not in issuers.columns and "state" in issuers.columns:
        issuers = issuers.rename(columns={"state": "state_code"})

    # Normalize date columns from Snowflake
    for df, cols in [
        (bonds, ["issue_date", "maturity_date"]),
        (trades, ["trade_date"]),
        (ratings, ["rating_date"]),
        (econ, ["date"]),
    ]:
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

    latest = (
        ratings.sort_values(["bond_id", "rating_date"])
        .groupby("bond_id")
        .tail(1)[["bond_id", "rating"]]
        .rename(columns={"rating": "latest_rating"})
    )
    df = (
        trades.merge(bonds, on="bond_id", how="left")
        .merge(issuers, on="issuer_id", how="left")
        .merge(latest, on="bond_id", how="left")
        .merge(purposes[["purpose_id", "purpose_category"]], on="purpose_id", how="left")
    )
    if "state_code" not in df.columns and "state" in df.columns:
        df["state_code"] = df["state"]
    df["time_to_maturity_years"] = (df["maturity_date"] - df["trade_date"]).dt.days / 365.25
    df = df[df["time_to_maturity_years"].between(0, 40)]
    return df, econ, purposes, source


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
if len(state_options) == 5:
    st.sidebar.caption(
        "Only five states are available (CA, FL, IL, NY, TX) because the sample dataset "
        "includes issuers and macro data for those geographies."
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


if not session:
    st.warning("Snowflake/Snowpark is not installed or no active session. Snowflake query tabs are disabled; local CSV visualizations remain available below.")
else:
    tabs = st.tabs([q[0] for q in QUERIES])
    for tab, (title, builder) in zip(tabs, QUERIES):
        with tab:
            st.subheader(title)
            info = QUERY_SUMMARIES.get(title)
            if info:
                st.markdown(
                    f"**Purpose:** {info['purpose']}\n\n"
                    f"**Tables/joins:** {info['tables']}\n\n"
                    f"**Filters:** {info['filters']}"
                )
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
                                color=alt.Color("purpose_category:N", title="Purpose"),
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
                                color=alt.Color(
                                    "total_quantity:Q",
                                    title="Total Quantity",
                                    scale=alt.Scale(scheme="blues"),
                                ),
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
                        counts = (
                            df_q3.groupby("migration")
                            .agg(
                                n_bonds=("bond_id", "nunique"),
                                bonds=(
                                    "bond_id",
                                    lambda s: ", ".join(sorted({str(b) for b in s if pd.notna(b)})),
                                ),
                            )
                            .reset_index()
                        )

                        chart = (
                            alt.Chart(counts)
                            .mark_bar()
                            .encode(
                                x=alt.X("migration:N", sort="-y", title="Rating Migration"),
                                y=alt.Y("n_bonds:Q", title="Number of Bonds"),
                                tooltip=["migration", "n_bonds", "bonds"],
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
                                y=alt.Y(
                                    "avg_coupon_spread:Q",
                                    title="Avg Coupon – 10Y Treasury",
                                    scale=alt.Scale(domain=[2.5, 3.5]),
                                ),
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


# ---------- LOCAL VISUALIZATIONS ----------
st.header("Interactive Visualizations")
viz_df, econ_df, purposes_df, source_label = load_visual_data()
st.caption(f"Source: {source_label}")

state_opts = sorted(viz_df["state_code"].dropna().unique().tolist())
rating_opts = sorted(viz_df["latest_rating"].dropna().unique().tolist())

col1, col2 = st.columns(2)
with col1:
    selected_states_local = st.multiselect("States", options=state_opts, default=state_opts)
with col2:
    selected_ratings = st.multiselect("Ratings", options=rating_opts, default=rating_opts)

filtered = viz_df[
    viz_df["state_code"].isin(selected_states_local) & viz_df["latest_rating"].isin(selected_ratings)
]
if filtered.empty:
    st.warning("No trades match the current filters.")
else:
    st.caption(f"{len(filtered):,} trades after filters (scatter capped at 8,000 points to reduce overplotting)")
    plot_df = filtered.sample(n=8000, random_state=1) if len(filtered) > 8000 else filtered

    # Yield curve scatter
    st.subheader("Yield Curve by Rating")
    yield_chart = (
        alt.Chart(plot_df)
        .mark_circle(opacity=0.55)
        .encode(
            x=alt.X("time_to_maturity_years:Q", title="Time to Maturity (years)"),
            y=alt.Y("yield:Q", title="Trade Yield (%)"),
            color=alt.Color("latest_rating:N", title="Latest Rating"),
            size=alt.Size("quantity:Q", title="Quantity", scale=alt.Scale(range=[15, 120])),
            tooltip=[
                "bond_id",
                "issuer_name",
                "state_code",
                "latest_rating",
                alt.Tooltip("time_to_maturity_years:Q", format=".1f"),
                alt.Tooltip("yield:Q", format=".2f"),
            ],
        )
        .properties(height=380)
    )
    st.altair_chart(yield_chart, use_container_width=True)

    # Rating distribution
    st.subheader("Credit Rating Distribution")
    rating_counts = (
        filtered.groupby("latest_rating")["bond_id"].nunique().reset_index(name="bonds")
    )
    rating_bar = (
        alt.Chart(rating_counts)
        .mark_bar()
        .encode(
            x=alt.X("latest_rating:N", title="Rating", sort="-y"),
            y=alt.Y("bonds:Q", title="Number of Bonds"),
            tooltip=["latest_rating", "bonds"],
        )
        .properties(height=300)
    )
    st.altair_chart(rating_bar, use_container_width=True)

    # State comparison with error bars
    st.subheader("State Comparison – Avg Price & Yield with Error Bars")
    state_stats = (
        filtered.groupby("state_code")
        .agg(
            avg_price=("trade_price", "mean"),
            std_price=("trade_price", "std"),
            avg_yield=("yield", "mean"),
            std_yield=("yield", "std"),
        )
        .reset_index()
        .fillna(0)
    )
    for col_name in ["price_low", "price_high", "yield_low", "yield_high"]:
        if col_name not in state_stats:
            state_stats[col_name] = 0.0
    state_stats["price_low"] = state_stats["avg_price"] - state_stats["std_price"]
    state_stats["price_high"] = state_stats["avg_price"] + state_stats["std_price"]
    state_stats["yield_low"] = state_stats["avg_yield"] - state_stats["std_yield"]
    state_stats["yield_high"] = state_stats["avg_yield"] + state_stats["std_yield"]

    price_chart = (
        alt.Chart(state_stats)
        .mark_errorbar()
        .encode(x="state_code:N", y="price_low:Q", y2="price_high:Q")
    ) + alt.Chart(state_stats).mark_point(filled=True, color="#59a14f").encode(
        x="state_code:N", y=alt.Y("avg_price:Q", title="Avg Price ($)")
    )
    yield_chart_state = (
        alt.Chart(state_stats)
        .mark_errorbar(color="#e15759")
        .encode(x="state_code:N", y="yield_low:Q", y2="yield_high:Q")
    ) + alt.Chart(state_stats).mark_point(filled=True, color="#e15759").encode(
        x="state_code:N", y=alt.Y("avg_yield:Q", title="Avg Yield (%)")
    )
    st.altair_chart(price_chart.properties(height=260), use_container_width=True)
    st.altair_chart(yield_chart_state.properties(height=260), use_container_width=True)

    # Time series overlay
    st.subheader("Time Series – Prices/Yields vs 10Y Treasury")
    monthly = (
        filtered.resample("MS", on="trade_date")
        .agg(avg_price=("trade_price", "mean"), avg_yield=("yield", "mean"))
        .reset_index()
    )
    econ_monthly = econ_df.groupby(pd.Grouper(key="date", freq="MS"))["treasury_10yr"].mean().reset_index()
    price_line = alt.Chart(monthly).mark_line(color="#4e79a7").encode(
        x=alt.X("trade_date:T", title="Month"), y=alt.Y("avg_price:Q", title="Price ($)")
    )
    yield_line = alt.Chart(monthly).mark_line(color="#e15759").encode(
        x="trade_date:T", y=alt.Y("avg_yield:Q", title="Yield (%)")
    )
    tenyr_line = alt.Chart(econ_monthly).mark_line(color="#59a14f", strokeDash=[4, 3]).encode(
        x=alt.X("date:T", title="Month"), y=alt.Y("treasury_10yr:Q", title="Yield (%)")
    )
    st.altair_chart(
        alt.layer(price_line, yield_line, tenyr_line).resolve_scale(y="independent").properties(height=320),
        use_container_width=True,
    )

    # Sector performance heatmap
    st.subheader("Sector Performance – Avg Yield by Purpose & State")
    sector = (
        filtered.groupby(["purpose_category", "state_code"])["yield"]
        .mean()
        .reset_index()
    )
    heatmap = (
        alt.Chart(sector)
        .mark_rect()
        .encode(
            x=alt.X("state_code:N", title="State"),
            y=alt.Y("purpose_category:N", title="Purpose"),
            color=alt.Color("yield:Q", title="Avg Yield (%)", scale=alt.Scale(scheme="orangered")),
            tooltip=["purpose_category", "state_code", alt.Tooltip("yield:Q", format=".2f")],
        )
        .properties(height=320)
    )
    st.altair_chart(heatmap, use_container_width=True)

    # Trading activity
    st.subheader("Trading Activity")
    monthly_qty = filtered.resample("MS", on="trade_date")["quantity"].sum().reset_index()
    buyer_mix = filtered.groupby("buyer_type")["quantity"].sum().reset_index()

    vol_chart = (
        alt.Chart(monthly_qty)
        .mark_line(point=True, color="#4e79a7")
        .encode(
            x=alt.X("trade_date:T", title="Month"),
            y=alt.Y("quantity:Q", title="Quantity Traded"),
            tooltip=["trade_date", "quantity"],
        )
        .properties(height=260)
    )
    buyer_bar = (
        alt.Chart(buyer_mix)
        .mark_bar()
        .encode(
            x=alt.X("buyer_type:N", title="Buyer Type"),
            y=alt.Y("quantity:Q", title="Quantity"),
            color=alt.Color("buyer_type:N", legend=None),
            tooltip=["buyer_type", "quantity"],
        )
        .properties(height=260)
    )
    st.altair_chart(vol_chart, use_container_width=True)
    st.altair_chart(buyer_bar, use_container_width=True)
    st.caption("Buyer type reflects trade-side labels in the transactions table and is a proxy for end-investor mix.")
