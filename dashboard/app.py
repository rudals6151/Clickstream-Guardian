"""
Clickstream Guardian Dashboard
Real-time monitoring dashboard using Streamlit
"""
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
from streamlit_autorefresh import st_autorefresh


# -----------------------------
# Config
# -----------------------------
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Clickstream Guardian",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------
# Theme / CSS (loaded from external file)
# -----------------------------
from pathlib import Path

_css_path = Path(__file__).parent / "styles.css"
st.markdown(f"<style>{_css_path.read_text()}</style>", unsafe_allow_html=True)


# -----------------------------
# Helpers
# -----------------------------
def fetch_api(endpoint, params=None):
    try:
        r = requests.get(f"{API_URL}{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API Error: {e}")
        return None

def kpi_card(title, value, help_text=None):
    st.markdown(
        f"""
        <div class="cg-card">
          <div class="cg-kpi-title">{title}</div>
          <div class="cg-kpi-value">{value}</div>
          {f'<div class="cg-kpi-help">{help_text}</div>' if help_text else ''}
        </div>
        """,
        unsafe_allow_html=True
    )

def apply_plotly_layout(fig, height=300):
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font=dict(color="#0f172a"),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(15,23,42,0.08)",
        zeroline=False
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(15,23,42,0.08)",
        zeroline=False
    )
    return fig


# -----------------------------
# Header
# -----------------------------
st.markdown(
    f"""
    <div class="cg-header">
      <div>
        <div class="cg-title">🛡️ Clickstream Guardian</div>
        <div class="cg-subtitle">Real-time anomaly monitoring & daily analytics</div>
      </div>
      <div class="cg-badge">API: {API_URL}</div>
    </div>
    """,
    unsafe_allow_html=True
)

# -----------------------------
# Sidebar
# -----------------------------
st.sidebar.markdown("### Navigation")
page = st.sidebar.radio(
    "Select Page",
    ["🏠 Overview", "🚨 Anomalies", "📊 Daily Metrics", "🔥 Popular Items"],
    label_visibility="collapsed"
)
st.sidebar.markdown("---")
st.sidebar.caption("Clickstream Guardian • Ops Dashboard")

# -----------------------------
# Pages
# -----------------------------
if page == "🏠 Overview":
    st.markdown('<div class="cg-section"><h3>Daily Overview</h3></div>', unsafe_allow_html=True)

    # Date selector
    metric_date = st.date_input("Select Date", value=datetime(2026, 1, 11))
    
    # Fetch daily metrics and funnel data
    daily_metric = fetch_api(f"/metrics/daily/{metric_date.isoformat()}")
    funnel_data = fetch_api(f"/metrics/funnel/{metric_date.isoformat()}")

    if daily_metric:
        # KPI Cards - Main Metrics
        st.markdown('<div class="cg-section"><h3>Key Metrics</h3></div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            kpi_card("Total Clicks", f"{int(daily_metric.get('total_clicks', 0)):,}", 
                     "Total click events")
        with c2:
            kpi_card("Total Purchases", f"{int(daily_metric.get('total_purchases', 0)):,}",
                     "Completed purchases")
        with c3:
            kpi_card("Conversion Rate", f"{daily_metric.get('conversion_rate', 0):.2%}",
                     "Purchase / Session ratio")
        with c4:
            kpi_card("Total Revenue", f"${int(daily_metric.get('total_revenue', 0)):,}",
                     "Total sales revenue")

        # Second row of KPIs - Engagement Metrics
        c5, c6, c7, c8 = st.columns(4)
        with c5:
            kpi_card("Unique Sessions", f"{int(daily_metric.get('unique_sessions', 0)):,}",
                     "Distinct user sessions")
        with c6:
            kpi_card("Avg Session Duration", f"{daily_metric.get('avg_session_duration_sec', 0):.0f}s",
                     "Average time per session")
        with c7:
            kpi_card("Avg Clicks/Session", f"{daily_metric.get('avg_clicks_per_session', 0):.2f}",
                     "Clicks per session")
        with c8:
            kpi_card("Avg Order Value", f"${daily_metric.get('avg_order_value', 0):.2f}",
                     "Average purchase amount")

        # Conversion Funnel
        if funnel_data and len(funnel_data) > 0:
            st.markdown('<div class="cg-section"><h3>Conversion Funnel</h3></div>', unsafe_allow_html=True)
            
            df_funnel = pd.DataFrame(funnel_data)
            
            # Funnel visualization
            col_left, col_right = st.columns([2, 1])
            
            with col_left:
                # Funnel bar chart
                fig = px.funnel(df_funnel, 
                               x='session_count', 
                               y='funnel_stage',
                               title='Session Flow')
                fig.update_layout(height=350)
                fig = apply_plotly_layout(fig, height=350)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            
            with col_right:
                # Funnel statistics
                st.markdown("#### Funnel Statistics")
                for _, row in df_funnel.iterrows():
                    st.markdown(f"""
                    <div class="cg-card" style="margin-bottom: 10px;">
                        <div style="font-weight: 600; color: var(--accent);">{row['funnel_stage']}</div>
                        <div style="font-size: 1.2rem; font-weight: 700;">{int(row['session_count']):,}</div>
                        <div style="font-size: 0.85rem; color: var(--muted);">
                            {row['percentage']:.1f}% of total
                            {f"<br>Drop: {row['drop_rate']:.1%}" if row['drop_rate'] > 0 else ""}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Detailed metrics table
        st.markdown('<div class="cg-section"><h3>Detailed Metrics</h3></div>', unsafe_allow_html=True)
        
        metrics_display = {
            "Metric": [
                "Total Clicks",
                "Total Purchases", 
                "Unique Sessions",
                "Unique Items",
                "Conversion Rate",
                "Avg Session Duration (sec)",
                "Avg Clicks per Session",
                "Total Revenue",
                "Avg Order Value"
            ],
            "Value": [
                f"{int(daily_metric.get('total_clicks', 0)):,}",
                f"{int(daily_metric.get('total_purchases', 0)):,}",
                f"{int(daily_metric.get('unique_sessions', 0)):,}",
                f"{int(daily_metric.get('unique_items', 0)):,}",
                f"{daily_metric.get('conversion_rate', 0):.4f}",
                f"{daily_metric.get('avg_session_duration_sec', 0):.2f}",
                f"{daily_metric.get('avg_clicks_per_session', 0):.2f}",
                f"${int(daily_metric.get('total_revenue', 0)):,}",
                f"${daily_metric.get('avg_order_value', 0):.2f}"
            ]
        }
        
        df_metrics = pd.DataFrame(metrics_display)
        st.dataframe(df_metrics, use_container_width=True, height=380, hide_index=True)
        
    else:
        st.warning(f"No data available for {metric_date.isoformat()}. Please select another date.")

elif page == "🚨 Anomalies":
    # Auto-refresh every 10 seconds
    st_autorefresh(interval=10_000, key="anomalies_refresh")

    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")


    st.markdown('<div class="cg-section"><h3>Anomaly Detection</h3></div>', unsafe_allow_html=True)

    f1, f2, f3 = st.columns([1, 1, 1])
    with f1:
        limit = st.selectbox("Limit", [50, 100, 200, 500], index=1)
    with f2:
        types = fetch_api("/anomalies/types")
        type_options = ["All"] + [t["anomaly_type"] for t in types] if types else ["All"]
        selected_type = st.selectbox("Anomaly Type", type_options)
    with f3:
        min_score = st.number_input("Min Score", min_value=0.0, value=0.0, step=0.5)

    params = {"limit": limit}
    if selected_type != "All":
        params["anomaly_type"] = selected_type
    if min_score > 0:
        params["min_score"] = min_score

    anomalies = fetch_api("/anomalies", params=params)

    if anomalies and len(anomalies) > 0:
        df = pd.DataFrame(anomalies)

        df["detected_at"]  = pd.to_datetime(df["detected_at"],  format="mixed", errors="coerce")
        df["window_start"] = pd.to_datetime(df["window_start"], format="mixed", errors="coerce")
        df["window_end"]   = pd.to_datetime(df["window_end"],   format="mixed", errors="coerce")


        # KPI Row
        k1, k2, k3 = st.columns(3)
        with k1:
            kpi_card("Total", f"{len(df):,}", "Rows returned by current filters")
        with k2:
            kpi_card("Avg Score", f"{df['anomaly_score'].mean():.2f}", "Average anomaly score")
        with k3:
            kpi_card("Max Clicks", f"{int(df['click_count'].max()):,}", "Maximum click count in returned rows")

        # Score overview (clean)
        st.markdown('<div class="cg-section"><h3>Score Overview</h3></div>', unsafe_allow_html=True)
        fig = px.box(df, y="anomaly_score", points="outliers")
        fig.update_xaxes(visible=False)
        fig.update_yaxes(title="Anomaly Score")
        fig = apply_plotly_layout(fig, height=280)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        # Table
        st.markdown('<div class="cg-section"><h3>Recent Sessions</h3></div>', unsafe_allow_html=True)
        view = df[[
            "session_id", "detected_at", "click_count",
            "unique_items", "anomaly_score", "anomaly_type"
        ]].sort_values("detected_at", ascending=False)

        st.dataframe(view, use_container_width=True, height=420)
    else:
        st.info("No anomalies found matching the criteria.")

elif page == "📊 Daily Metrics":
    st.markdown('<div class="cg-section"><h3>Daily Metrics</h3></div>', unsafe_allow_html=True)

    d1, d2 = st.columns(2)
    with d1:
        start_date = st.date_input("Start Date", value=datetime(2026, 1, 11))
    with d2:
        end_date = st.date_input("End Date", value=datetime(2026, 1, 11))

    metrics = fetch_api("/metrics/daily", params={
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat()
    })

    if metrics and len(metrics) > 0:
        df = pd.DataFrame(metrics)
        df["metric_date"] = pd.to_datetime(df["metric_date"])

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            kpi_card("Total Clicks", f"{int(df['total_clicks'].sum()):,}")
        with c2:
            kpi_card("Total Purchases", f"{int(df['total_purchases'].sum()):,}")
        with c3:
            kpi_card("Avg Conversion", f"{df['conversion_rate'].mean():.2%}")
        with c4:
            kpi_card("Total Revenue", f"${int(df['total_revenue'].sum()):,}")

        st.markdown('<div class="cg-section"><h3>Details</h3></div>', unsafe_allow_html=True)
        st.dataframe(df.sort_values("metric_date", ascending=False), use_container_width=True, height=520)
    else:
        st.warning("No metrics found for the selected date range.")

elif page == "🔥 Popular Items":
    st.markdown('<div class="cg-section"><h3>Popular Items & Categories</h3></div>', unsafe_allow_html=True)

    metric_date = st.date_input("Select Date", value=datetime(2026, 1, 11))
    top_n = st.slider("Number of Items", 1, 10, 10)

    items = fetch_api(f"/items/popular/{metric_date.isoformat()}", params={"limit": top_n})
    categories = fetch_api(f"/items/categories/{metric_date.isoformat()}")

    left, right = st.columns(2)

    with left:
        st.markdown('<div class="cg-section"><h3>Top Items</h3></div>', unsafe_allow_html=True)
        if items and len(items) > 0:
            df_items = pd.DataFrame(items)
            top_items = df_items.head(10)

            fig = px.bar(top_items, x="revenue", y="item_id", orientation="h")
            fig.update_yaxes(type="category", title=None)
            fig.update_xaxes(title="Revenue ($)")
            fig = apply_plotly_layout(fig, height=320)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

            st.dataframe(df_items, use_container_width=True, height=420)
        else:
            st.info("No popular items data available.")

    with right:
        st.markdown('<div class="cg-section"><h3>Top Categories</h3></div>', unsafe_allow_html=True)
        if categories and len(categories) > 0:
            df_cat = pd.DataFrame(categories).head(10)
            fig = px.pie(df_cat, values="revenue", names="category", hole=0.55)
            fig = apply_plotly_layout(fig, height=320)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.dataframe(pd.DataFrame(categories), use_container_width=True, height=420)
        else:
            st.info("No category data available.")

# Footer
st.markdown("---")
st.caption(f"Clickstream Guardian Dashboard • v1.0.0 • API Docs: {API_URL}/docs")
