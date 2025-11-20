import streamlit as st
import pandas as pd

st.title("Muni Bonds Dashboard")
st.caption("Replace the query with your muni bond table/view")

# Use the Snowflake-native session provided by Snowsight.
conn = st.connection("snowflake")

@st.cache_data
def load_data():
    # TODO: replace table name with your muni data source
    with conn.session() as session:
        return session.sql("SELECT * FROM gator_db.muni.your_muni_table").to_pandas()

try:
    df = load_data()
    st.metric("Rows", len(df))
    st.dataframe(df)
except Exception as exc:
    st.error("Update the query/table and ensure secrets/connections are set.")
    st.exception(exc)
