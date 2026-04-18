"""
Perk Analytics Dashboard
========================
Professional Streamlit dashboard visualising travel & spend analytics
from the dbt marts layer (DuckDB backend).

Brand colours:
  - Perk Teal / Blue:  #0066CC
  - Dark Navy:         #1A1A2E
  - Accent Green:      #00C896
  - Light Grey:        #F5F7FA
"""

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Config & constants
# ---------------------------------------------------------------------------

BRAND_BLUE   = "#0066CC"
BRAND_NAVY   = "#1A1A2E"
BRAND_GREEN  = "#00C896"
BRAND_GREY   = "#F5F7FA"
BRAND_ORANGE = "#FF6B35"

COLOR_SEQ = [BRAND_BLUE, BRAND_GREEN, BRAND_ORANGE, "#9B59B6", "#E74C3C",
             "#F39C12", "#1ABC9C", "#3498DB"]

# Resolve DuckDB path relative to this file's location
_PROJECT_ROOT = Path(__file__).parent.parent
_DB_PATH = str(_PROJECT_ROOT / "dbt_project" / "dev.duckdb")

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Perk Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for brand styling
st.markdown(f"""
<style>
    .main {{ background-color: {BRAND_GREY}; }}
    .stMetric {{
        background-color: white;
        border-radius: 10px;
        padding: 16px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }}
    .stMetric label {{ color: {BRAND_NAVY}; font-weight: 600; }}
    .stMetric [data-testid="stMetricValue"] {{
        color: {BRAND_BLUE};
        font-size: 1.8rem;
        font-weight: 700;
    }}
    h1, h2, h3 {{ color: {BRAND_NAVY}; }}
    .sidebar .sidebar-content {{ background-color: {BRAND_NAVY}; }}
    section[data-testid="stSidebar"] {{
        background-color: {BRAND_NAVY};
    }}
    section[data-testid="stSidebar"] * {{
        color: white !important;
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_resource
def get_connection():
    if not Path(_DB_PATH).exists():
        st.error(
            f"DuckDB file not found at `{_DB_PATH}`. "
            "Please run `dbt run` first to populate the mart tables."
        )
        st.stop()
    return duckdb.connect(_DB_PATH, read_only=True)


@st.cache_data(ttl=300)
def load_table(table_name: str) -> pd.DataFrame:
    con = get_connection()
    # dbt creates schemas; try marts schema first, fall back to main
    for schema in ("main_marts", "marts", "main"):
        try:
            df = con.execute(f"SELECT * FROM {schema}.{table_name}").df()
            return df
        except Exception:
            continue
    st.error(f"Could not load table `{table_name}` from DuckDB. Run `dbt run` first.")
    st.stop()


def fmt_eur(value: float) -> str:
    if value >= 1_000_000:
        return f"€{value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"€{value/1_000:.0f}K"
    return f"€{value:.0f}"


def pct(value: float) -> str:
    return f"{value:.1f}%"


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(f"""
    <div style='text-align:center; padding: 20px 0;'>
        <h2 style='color:white; font-size:1.4rem; margin:0;'>✈️ Perk Analytics</h2>
        <p style='color:#8899BB; font-size:0.8rem; margin:4px 0 0 0;'>Travel & Spend Intelligence</p>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        options=[
            "Executive Overview",
            "Travel Analytics",
            "Expense Analytics",
            "Company Health",
        ],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown(
        "<p style='color:#8899BB; font-size:0.75rem;'>Built with dbt + DuckDB<br/>Data: synthetic (seed=42)</p>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Load data once
# ---------------------------------------------------------------------------

fct_bookings  = load_table("fct_bookings")
fct_expenses  = load_table("fct_expenses")
dim_employees = load_table("dim_employees")
dim_companies = load_table("dim_companies")

# Ensure date columns are datetime
for col in ["booking_date", "travel_date", "booking_month"]:
    if col in fct_bookings.columns:
        fct_bookings[col] = pd.to_datetime(fct_bookings[col])

for col in ["expense_date", "expense_month"]:
    if col in fct_expenses.columns:
        fct_expenses[col] = pd.to_datetime(fct_expenses[col])


# ===========================================================================
# PAGE 1: Executive Overview
# ===========================================================================

if page == "Executive Overview":
    st.title("Executive Overview")
    st.markdown("High-level KPIs and business health metrics across the Perk platform.")

    # --- KPI Cards ---
    total_bookings    = len(fct_bookings)
    total_spend_eur   = fct_bookings["cost_eur"].sum()
    avg_booking_value = fct_bookings["cost_eur"].mean()
    policy_compliance = fct_bookings["is_policy_compliant"].mean() * 100
    total_expenses    = fct_expenses["cost_eur"].sum()
    expense_approval  = (fct_expenses["status"] == "approved").mean() * 100

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("Total Bookings", f"{total_bookings:,}")
    with col2:
        st.metric("Total Booking Spend", fmt_eur(total_spend_eur))
    with col3:
        st.metric("Avg Booking Value", fmt_eur(avg_booking_value))
    with col4:
        st.metric("Policy Compliance", pct(policy_compliance))
    with col5:
        st.metric("Total Expenses", fmt_eur(total_expenses))
    with col6:
        st.metric("Expense Approval Rate", pct(expense_approval))

    st.markdown("---")

    # --- Row 2: Revenue by company size + spend by category ---
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Booking Spend by Company Size")
        spend_by_tier = (
            fct_bookings.groupby("size_tier")["cost_eur"]
            .sum()
            .reset_index()
            .sort_values("cost_eur", ascending=False)
        )
        fig = px.bar(
            spend_by_tier,
            x="size_tier", y="cost_eur",
            color="size_tier",
            color_discrete_sequence=COLOR_SEQ,
            labels={"size_tier": "Company Tier", "cost_eur": "Total Spend (EUR)"},
            text_auto=".2s",
        )
        fig.update_layout(showlegend=False, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Expense Spend by Category")
        spend_by_cat = (
            fct_expenses.groupby("category")["cost_eur"]
            .sum()
            .reset_index()
            .sort_values("cost_eur", ascending=False)
        )
        fig = px.pie(
            spend_by_cat,
            values="cost_eur", names="category",
            color_discrete_sequence=COLOR_SEQ,
            hole=0.4,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 3: Monthly spend trend ---
    st.subheader("Monthly Booking Spend Trend")
    monthly = (
        fct_bookings.groupby("booking_month")["cost_eur"]
        .sum()
        .reset_index()
        .rename(columns={"booking_month": "Month", "cost_eur": "Total Spend (EUR)"})
    )
    fig = px.area(
        monthly, x="Month", y="Total Spend (EUR)",
        color_discrete_sequence=[BRAND_BLUE],
        labels={"Month": "", "Total Spend (EUR)": "Spend (EUR)"},
    )
    fig.update_traces(fill="tozeroy", line_color=BRAND_BLUE, fillcolor="rgba(0,102,204,0.15)")
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# PAGE 2: Travel Analytics
# ===========================================================================

elif page == "Travel Analytics":
    st.title("Travel Analytics")
    st.markdown("Deep-dive into booking trends, destinations, and policy adherence.")

    col1, col2, col3, col4 = st.columns(4)
    confirmed_pct = (fct_bookings["status"] == "confirmed").mean() * 100
    cancelled_pct = (fct_bookings["status"] == "cancelled").mean() * 100
    avg_advance   = fct_bookings["advance_days"].mean()
    flight_share  = (fct_bookings["trip_type"] == "flight").mean() * 100

    with col1:
        st.metric("Confirmed Rate", pct(confirmed_pct))
    with col2:
        st.metric("Cancellation Rate", pct(cancelled_pct))
    with col3:
        st.metric("Avg Advance Days", f"{avg_advance:.0f} days")
    with col4:
        st.metric("Flight Share", pct(flight_share))

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Bookings by Month")
        monthly_cnt = (
            fct_bookings.groupby("booking_month")
            .agg(bookings=("booking_id", "count"))
            .reset_index()
        )
        fig = px.bar(
            monthly_cnt, x="booking_month", y="bookings",
            color_discrete_sequence=[BRAND_BLUE],
            labels={"booking_month": "Month", "bookings": "Number of Bookings"},
        )
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Trip Type Mix")
        trip_mix = fct_bookings["trip_type"].value_counts().reset_index()
        trip_mix.columns = ["trip_type", "count"]
        fig = px.pie(
            trip_mix,
            values="count", names="trip_type",
            color_discrete_sequence=COLOR_SEQ,
            hole=0.35,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader("Top 10 Destination Cities")
        top_dest = (
            fct_bookings.groupby("destination_city")
            .agg(bookings=("booking_id", "count"), spend=("cost_eur", "sum"))
            .sort_values("bookings", ascending=False)
            .head(10)
            .reset_index()
        )
        fig = px.bar(
            top_dest,
            x="bookings", y="destination_city",
            orientation="h",
            color="spend",
            color_continuous_scale=["#BDD7F5", BRAND_BLUE],
            labels={"destination_city": "", "bookings": "Bookings", "spend": "Spend (EUR)"},
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r2:
        st.subheader("Advance Booking Distribution")
        tier_counts = fct_bookings["advance_booking_tier"].value_counts().reset_index()
        tier_counts.columns = ["tier", "count"]
        tier_order = ["last_minute", "standard", "advance"]
        tier_counts["tier"] = pd.Categorical(tier_counts["tier"], categories=tier_order, ordered=True)
        tier_counts = tier_counts.sort_values("tier")
        fig = px.bar(
            tier_counts,
            x="tier", y="count",
            color="tier",
            color_discrete_sequence=[BRAND_ORANGE, BRAND_BLUE, BRAND_GREEN],
            labels={"tier": "Booking Tier", "count": "Number of Bookings"},
        )
        fig.update_layout(showlegend=False, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    # Policy compliance heatmap by department and trip type
    st.subheader("Policy Compliance by Department & Trip Type")
    compliance_pivot = (
        fct_bookings.groupby(["department", "trip_type"])["is_policy_compliant"]
        .mean()
        .mul(100)
        .reset_index()
        .pivot(index="department", columns="trip_type", values="is_policy_compliant")
    )
    fig = px.imshow(
        compliance_pivot,
        color_continuous_scale=["#FFE0E0", "#FFFFFF", "#D0F0E0"],
        zmin=50, zmax=100,
        text_auto=".0f",
        labels={"color": "Compliance %"},
        aspect="auto",
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# PAGE 3: Expense Analytics
# ===========================================================================

elif page == "Expense Analytics":
    st.title("Expense Analytics")
    st.markdown("Employee expense patterns, approval rates, and policy compliance.")

    col1, col2, col3, col4 = st.columns(4)
    total_exp_count   = len(fct_expenses)
    total_exp_spend   = fct_expenses["cost_eur"].sum()
    receipt_rate      = fct_expenses["receipt_attached"].mean() * 100
    exp_policy_rate   = fct_expenses["is_policy_compliant"].mean() * 100

    with col1:
        st.metric("Total Expenses", f"{total_exp_count:,}")
    with col2:
        st.metric("Total Expense Spend", fmt_eur(total_exp_spend))
    with col3:
        st.metric("Receipt Attached Rate", pct(receipt_rate))
    with col4:
        st.metric("Policy Compliance", pct(exp_policy_rate))

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Expense Spend by Category & Department")
        cat_dept = (
            fct_expenses.groupby(["department", "category"])["cost_eur"]
            .sum()
            .reset_index()
        )
        fig = px.bar(
            cat_dept,
            x="department", y="cost_eur",
            color="category",
            barmode="stack",
            color_discrete_sequence=COLOR_SEQ,
            labels={"department": "Department", "cost_eur": "Spend (EUR)", "category": "Category"},
        )
        fig.update_layout(
            xaxis_tickangle=-30,
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Approval Status Distribution")
        status_counts = fct_expenses["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig = px.pie(
            status_counts,
            values="count", names="status",
            color="status",
            color_discrete_map={
                "approved": BRAND_GREEN,
                "rejected": "#E74C3C",
                "pending": BRAND_ORANGE,
            },
            hole=0.4,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader("Policy Compliance by Seniority")
        seniority_compliance = (
            fct_expenses.groupby("seniority")["is_policy_compliant"]
            .mean()
            .mul(100)
            .reset_index()
            .rename(columns={"is_policy_compliant": "compliance_pct"})
        )
        seniority_order = ["junior", "mid", "senior", "manager", "director"]
        seniority_compliance["seniority"] = pd.Categorical(
            seniority_compliance["seniority"], categories=seniority_order, ordered=True
        )
        seniority_compliance = seniority_compliance.sort_values("seniority")
        fig = px.bar(
            seniority_compliance,
            x="seniority", y="compliance_pct",
            color="compliance_pct",
            color_continuous_scale=["#FFB3B3", BRAND_BLUE],
            range_color=[70, 100],
            text_auto=".1f",
            labels={"seniority": "Seniority", "compliance_pct": "Compliance (%)"},
        )
        fig.update_layout(
            showlegend=False, plot_bgcolor="white", paper_bgcolor="white",
            yaxis_range=[0, 100],
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r2:
        st.subheader("Monthly Expense Trend")
        monthly_exp = (
            fct_expenses.groupby("expense_month")
            .agg(total_spend=("cost_eur", "sum"), count=("expense_id", "count"))
            .reset_index()
        )
        fig = px.line(
            monthly_exp,
            x="expense_month", y="total_spend",
            color_discrete_sequence=[BRAND_GREEN],
            labels={"expense_month": "Month", "total_spend": "Total Spend (EUR)"},
            markers=True,
        )
        fig.update_traces(line_width=2.5)
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    # Receipt compliance table
    st.subheader("Receipt Attachment Rate by Category")
    receipt_cat = (
        fct_expenses.groupby("category")
        .agg(
            total=("expense_id", "count"),
            with_receipt=("receipt_attached", "sum"),
        )
        .assign(receipt_rate=lambda df: (df["with_receipt"] / df["total"] * 100).round(1))
        .reset_index()
        .sort_values("receipt_rate", ascending=False)
    )
    receipt_cat.columns = ["Category", "Total Expenses", "With Receipt", "Receipt Rate (%)"]
    st.dataframe(receipt_cat, use_container_width=True, hide_index=True)


# ===========================================================================
# PAGE 4: Company Health
# ===========================================================================

elif page == "Company Health":
    st.title("Company Health")
    st.markdown("Customer-level MRR analysis, booking activity, and company segmentation.")

    col1, col2, col3, col4 = st.columns(4)
    total_companies   = len(dim_companies)
    total_mrr         = dim_companies["monthly_mrr_eur"].sum()
    avg_mrr           = dim_companies["monthly_mrr_eur"].mean()
    enterprise_count  = (dim_companies["size_tier"] == "Enterprise").sum()

    with col1:
        st.metric("Total Companies", f"{total_companies:,}")
    with col2:
        st.metric("Total MRR", fmt_eur(total_mrr))
    with col3:
        st.metric("Avg MRR per Company", fmt_eur(avg_mrr))
    with col4:
        st.metric("Enterprise Customers", f"{enterprise_count:,}")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("MRR by Company Tier")
        mrr_by_tier = (
            dim_companies.groupby("size_tier")["monthly_mrr_eur"]
            .sum()
            .reset_index()
            .sort_values("monthly_mrr_eur", ascending=False)
        )
        fig = px.bar(
            mrr_by_tier,
            x="size_tier", y="monthly_mrr_eur",
            color="size_tier",
            color_discrete_sequence=COLOR_SEQ,
            text_auto=".2s",
            labels={"size_tier": "Tier", "monthly_mrr_eur": "Total MRR (EUR)"},
        )
        fig.update_layout(showlegend=False, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Bookings per Company (Top 20)")
        bookings_per_co = (
            fct_bookings.groupby(["company_id", "company_name"])
            .agg(bookings=("booking_id", "count"), spend=("cost_eur", "sum"))
            .reset_index()
            .sort_values("bookings", ascending=False)
            .head(20)
        )
        fig = px.bar(
            bookings_per_co,
            x="company_name", y="bookings",
            color="spend",
            color_continuous_scale=["#BDD7F5", BRAND_NAVY],
            labels={"company_name": "", "bookings": "Bookings", "spend": "Spend (EUR)"},
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    # MRR vs Booking Spend scatter
    st.subheader("MRR vs. Total Booking Spend by Company")
    co_spend = (
        fct_bookings.groupby("company_id")["cost_eur"]
        .sum()
        .reset_index()
        .rename(columns={"cost_eur": "total_booking_spend"})
    )
    co_merged = dim_companies.merge(co_spend, on="company_id", how="left").fillna(0)
    fig = px.scatter(
        co_merged,
        x="monthly_mrr_eur", y="total_booking_spend",
        color="size_tier",
        size="total_booking_spend",
        hover_data=["company_name", "industry", "country"],
        color_discrete_sequence=COLOR_SEQ,
        labels={
            "monthly_mrr_eur": "Monthly MRR (EUR)",
            "total_booking_spend": "Total Booking Spend (EUR)",
            "size_tier": "Tier",
        },
        size_max=40,
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)

    # Top companies table
    st.subheader("Top 10 Companies by Total Spend")
    top_cos = (
        fct_bookings.groupby(["company_id", "company_name", "industry", "size_tier"])
        .agg(
            total_bookings=("booking_id", "count"),
            total_booking_spend=("cost_eur", "sum"),
        )
        .reset_index()
        .merge(
            fct_expenses.groupby("company_id")["cost_eur"].sum()
            .reset_index().rename(columns={"cost_eur": "total_expense_spend"}),
            on="company_id", how="left",
        )
        .fillna(0)
        .assign(total_spend=lambda df: df["total_booking_spend"] + df["total_expense_spend"])
        .sort_values("total_spend", ascending=False)
        .head(10)
        [["company_name", "industry", "size_tier", "total_bookings", "total_booking_spend", "total_expense_spend", "total_spend"]]
    )
    top_cos.columns = ["Company", "Industry", "Tier", "Bookings", "Booking Spend (EUR)", "Expense Spend (EUR)", "Total Spend (EUR)"]
    for col in ["Booking Spend (EUR)", "Expense Spend (EUR)", "Total Spend (EUR)"]:
        top_cos[col] = top_cos[col].round(0).astype(int)
    st.dataframe(top_cos, use_container_width=True, hide_index=True)
