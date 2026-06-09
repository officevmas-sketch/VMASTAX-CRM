
import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="VMAS CRM", layout="wide")

st.title("VMAS CRM Dashboard")

# Sample Data
funnel_df = pd.DataFrame({
    "Status": ["New Lead", "In Progress", "Completed"],
    "Clients": [10, 7, 5]
})

# Funnel Chart
fig = px.bar(
    funnel_df,
    x="Clients",
    y="Status",
    orientation="h",
    text="Clients"
)

# Updated Premium Gold Styling
fig.update_traces(
    marker_color="#C9A227",
    marker_line_color="#B8860B",
    marker_line_width=1.5,
    opacity=0.9,
    textposition="outside"
)

fig.update_layout(
    title="Work Status Funnel",
    plot_bgcolor="white",
    paper_bgcolor="white",
    font=dict(color="#2E4053", size=14),
    xaxis_title="Clients",
    yaxis_title="",
    title_font=dict(size=22),
    height=500
)

st.plotly_chart(fig, use_container_width=True)
