import streamlit as st

# ✅ MUST be the first Streamlit command
st.set_page_config(page_title="Hotel Dashboard", layout="wide")

import pandas as pd
import psycopg2
import os
import pydeck as pdk
import plotly.express as px

# Connect using psycopg2
@st.cache_data
def load_data():
    conn = psycopg2.connect(
        host="172.22.0.4",
        port="5432",
        dbname="hotel",
        user="airflow",
        password="airflow"
    )
    query = "SELECT * FROM hotel_bookings"
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = df.columns.str.strip()  # Clean up any whitespace in column names
    return df

df = load_data()

st.title("🏨 Hotel Booking Dashboard / ホテル予約ダッシュボード")

# Sidebar filters
st.sidebar.header("Filters")
hotel_filter = st.sidebar.multiselect("Hotel Type", df['hotel'].unique(), default=df['hotel'].unique())
year_filter = st.sidebar.multiselect("Arrival Year", sorted(df['arrival_date_year'].unique()), default=df['arrival_date_year'].unique())

# Apply filters
filtered_df = df[df['hotel'].isin(hotel_filter) & df['arrival_date_year'].isin(year_filter)]

# KPI Summary
total_bookings = len(filtered_df)
cancellations = filtered_df['is_canceled'].sum()
avg_adr = filtered_df['adr'].mean()
revenue = filtered_df[filtered_df['is_canceled'] == 0]['adr'].sum()

st.markdown("### 🔢 Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Bookings", total_bookings)
col2.metric("Cancellations", cancellations)
col3.metric("Avg. Daily Rate (ADR)", f"${avg_adr:.2f}")
col4.metric("Revenue (estimated)", f"${revenue:.0f}")

# Visualization: Guest Origin Map
st.markdown("### 🌍 Guest Origin Map (Country)")
guest_by_country = filtered_df['country'].value_counts().reset_index()
guest_by_country.columns = ['country', 'guest_count']

fig_map = px.choropleth(
    guest_by_country,
    locations='country',
    locationmode='country names',
    color='guest_count',
    color_continuous_scale='Tealgrn',
    title='Guest Distribution by Country'
)
st.plotly_chart(fig_map, use_container_width=True)

# Visualization: Booking Trends
st.markdown("### 📈 Monthly Booking Trends")
monthly = filtered_df.groupby(['arrival_date_month'])['is_canceled'].count().reindex([
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
])
fig_month = px.bar(
    monthly,
    x=monthly.index,
    y='is_canceled',
    labels={'x': 'Month', 'is_canceled': 'Bookings'},
    title='Bookings per Month',
    color='is_canceled',
    color_discrete_sequence=['indigo']
)
st.plotly_chart(fig_month, use_container_width=True)

# Visualization: Repeated Guests
st.markdown("### 🔁 Guest Types")
guest_type = filtered_df['is_repeated_guest'].value_counts().rename({0: 'New Guests', 1: 'Repeated Guests'})
fig_guests = px.pie(
    names=guest_type.index,
    values=guest_type.values,
    title='New vs Repeated Guests',
    color_discrete_sequence=px.colors.sequential.Agsunset
)
st.plotly_chart(fig_guests, use_container_width=True)

# Visualization: Deposit Types
st.markdown("### 💰 Deposit Type Distribution")
fig_deposit = px.histogram(
    filtered_df,
    x='deposit_type',
    color='deposit_type',
    title='Distribution of Deposit Types',
    color_discrete_sequence=px.colors.qualitative.Prism
)
st.plotly_chart(fig_deposit, use_container_width=True)

# Visualization: Market Segments
st.markdown("### 🛫 Market Segment Analysis")
market = filtered_df['market_segment'].value_counts().reset_index()
market.columns = ['segment', 'count']
fig_market = px.bar(
    market,
    x='segment',
    y='count',
    title='Market Segment Distribution',
    color='count',
    color_continuous_scale='Viridis'
)
st.plotly_chart(fig_market, use_container_width=True)