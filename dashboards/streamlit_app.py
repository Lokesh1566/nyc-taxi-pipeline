"""
Streamlit dashboard for the NYC taxi pipeline.

Pulls from Snowflake gold tables. Refreshes every 30 seconds via st.cache_data's
ttl parameter — not true real-time but close enough for most use cases.

Run with: streamlit run dashboards/streamlit_app.py
"""

from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
import yaml


# --------------------------------------------------------------------
# Page config — call this first, before any other st.* commands
# --------------------------------------------------------------------
st.set_page_config(
    page_title="NYC Taxi Pipeline — Live",
    page_icon="🚕",
    layout="wide",
)


# --------------------------------------------------------------------
# Snowflake connection (cached, refreshed if creds change)
# --------------------------------------------------------------------
@st.cache_resource
def get_connection():
    with open("config/snowflake_config.yaml") as f:
        cfg = yaml.safe_load(f)
    return snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        login_timeout=30,
    )


@st.cache_data(ttl=30)  # 30 second cache = pseudo real-time
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn)


# --------------------------------------------------------------------
# Header
# --------------------------------------------------------------------
st.title("🚕 NYC Taxi Pipeline")
st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')} · data from Snowflake")


# --------------------------------------------------------------------
# Top-line KPIs
# --------------------------------------------------------------------
kpi_sql = """
SELECT
    SUM(TRIP_COUNT) AS total_trips,
    SUM(TOTAL_REVENUE) AS total_revenue,
    AVG(AVG_DISTANCE) AS avg_distance,
    AVG(AVG_DURATION_MIN) AS avg_duration
FROM FCT_TRIPS_HOURLY
WHERE PICKUP_HOUR_TS >= DATEADD(day, -7, CURRENT_TIMESTAMP())
"""

try:
    kpis = run_query(kpi_sql).iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Trips (7d)", f"{int(kpis['TOTAL_TRIPS']):,}")
    k2.metric("Revenue (7d)", f"${kpis['TOTAL_REVENUE']:,.0f}")
    k3.metric("Avg Distance", f"{kpis['AVG_DISTANCE']:.2f} mi")
    k4.metric("Avg Duration", f"{kpis['AVG_DURATION']:.1f} min")
except Exception as e:
    st.error(f"could not load KPIs: {e}")
    st.info("is your pipeline loaded into Snowflake? see README for setup")
    st.stop()


st.divider()


# --------------------------------------------------------------------
# Hourly trip volume
# --------------------------------------------------------------------
st.subheader("Trip Volume by Hour (last 24h)")

hourly_sql = """
SELECT PICKUP_HOUR_TS, SUM(TRIP_COUNT) AS trips, SUM(TOTAL_REVENUE) AS revenue
FROM FCT_TRIPS_HOURLY
WHERE PICKUP_HOUR_TS >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY PICKUP_HOUR_TS
ORDER BY PICKUP_HOUR_TS
"""
hourly = run_query(hourly_sql)

if not hourly.empty:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hourly["PICKUP_HOUR_TS"], y=hourly["TRIPS"],
        name="trips", mode="lines+markers",
    ))
    fig.update_layout(height=350, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("no data in the last 24 hours")


# --------------------------------------------------------------------
# Two-column layout: borough revenue + top zones
# --------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by Pickup Borough (7d)")
    borough_sql = """
    SELECT PICKUP_BOROUGH, SUM(TOTAL_REVENUE) AS revenue, SUM(TRIP_COUNT) AS trips
    FROM FCT_TRIPS_HOURLY
    WHERE PICKUP_HOUR_TS >= DATEADD(day, -7, CURRENT_TIMESTAMP())
      AND PICKUP_BOROUGH IS NOT NULL
    GROUP BY PICKUP_BOROUGH
    ORDER BY revenue DESC
    """
    borough_df = run_query(borough_sql)
    if not borough_df.empty:
        fig = px.bar(
            borough_df, x="PICKUP_BOROUGH", y="REVENUE",
            labels={"REVENUE": "revenue ($)", "PICKUP_BOROUGH": ""},
        )
        fig.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top 10 Busiest Pickup Zones (7d)")
    zones_sql = """
    SELECT PICKUP_ZONE, PICKUP_BOROUGH, SUM(TRIP_COUNT) AS trips
    FROM AGG_ZONE_STATS
    WHERE PICKUP_DATE >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY PICKUP_ZONE, PICKUP_BOROUGH
    ORDER BY trips DESC
    LIMIT 10
    """
    zones_df = run_query(zones_sql)
    if not zones_df.empty:
        fig = px.bar(
            zones_df, x="TRIPS", y="PICKUP_ZONE",
            orientation="h", color="PICKUP_BOROUGH",
            labels={"TRIPS": "trips", "PICKUP_ZONE": ""},
        )
        fig.update_layout(
            height=400, margin=dict(l=0, r=0, t=20, b=0),
            yaxis={"categoryorder": "total ascending"},
        )
        st.plotly_chart(fig, use_container_width=True)


# --------------------------------------------------------------------
# Payment breakdown
# --------------------------------------------------------------------
st.subheader("Payment Method Distribution (30d)")
payment_sql = """
SELECT PAYMENT_METHOD, SUM(TRIP_COUNT) AS trips
FROM AGG_PAYMENT_BREAKDOWN
WHERE PICKUP_DATE >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY PAYMENT_METHOD
ORDER BY trips DESC
"""
payment_df = run_query(payment_sql)
if not payment_df.empty:
    fig = px.pie(payment_df, values="TRIPS", names="PAYMENT_METHOD", hole=0.4)
    fig.update_layout(height=350, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig, use_container_width=True)


# --------------------------------------------------------------------
# Footer
# --------------------------------------------------------------------
st.divider()
st.caption(
    "Data pipeline built with PySpark, Airflow, and Snowflake. "
    "Source: NYC TLC Trip Record Data. "
    "Repo: [github.com/Lokesh1566/nyc-taxi-pipeline](https://github.com/Lokesh1566/nyc-taxi-pipeline)"
)
