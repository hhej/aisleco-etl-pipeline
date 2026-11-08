"""Aisle & Co. dashboard: both business problems and the conversion model.
Reads only the mart views of the warehouse (ETL_DW_DSN, default dbname=aisleco_dw).

  .venv/bin/streamlit run dashboard/app.py
"""
import sys
from decimal import Decimal
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl import db  # noqa: E402
from etl.config import Config  # noqa: E402

st.set_page_config(page_title="Aisle & Co. dashboard", layout="wide")


@st.cache_data(ttl=300)
def read(view, where=""):
    cfg = Config()
    with db.connect(cfg.dw_dsn, cfg, autocommit=True) as c:
        df = pd.DataFrame(c.execute(f"SELECT * FROM mart.{view} {where}").fetchall())
    for col in df.columns:                        # Postgres numeric arrives as Decimal; charts need floats
        if df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].astype(float)
    return df


st.title("Aisle & Co.")
st.caption("Weekly meeting view. Data: the warehouse's mart views, refreshed by the daily ETL and model run.")
p1, p2, follow, health = st.tabs(["Problem 1: capacity", "Problem 2: conversion", "Follow-up today", "Model health"])

with p1:
    wl = read("v_weekly_workload", "WHERE role IN ('Lead Planner','Planner','Coordinator','Event Staff')")
    wl["week_start"] = pd.to_datetime(wl.week_start)
    wl["util"] = wl.UtilizationRate.astype(float) * 100
    lo, hi = st.slider("Weeks", min_value=wl.week_start.min().date(), max_value=wl.week_start.max().date(),
                       value=(pd.Timestamp("2025-01-01").date(), pd.Timestamp("2026-12-31").date()))
    w = wl[wl.week_start.between(pd.Timestamp(lo), pd.Timestamp(hi))]
    peak = read("v_peak_acceptance").iloc[0]
    a, b, c = st.columns(3)
    a.metric("Employee-weeks over 100%", int((w.util > 100).sum()), help="Ops staff, weeks in the selected range")
    b.metric("Peak-date acceptance", f"{float(peak.acceptance_rate):.0%}", help="Inquiries for Nov-Feb or auspicious dates that booked")
    c.metric("Lost: date unavailable", int(peak.lost_date_unavailable))
    st.subheader("Weekly utilization, ops staff")
    st.altair_chart(alt.Chart(w).mark_rect().encode(
        x=alt.X("yearmonthdate(week_start):O", title=None, axis=alt.Axis(format="%b %Y", formatType="time", labelAngle=0, labelOverlap="greedy")),
        y=alt.Y("employee:N", title=None),
        color=alt.Color("util:Q", title="% of standard hours", scale=alt.Scale(domain=[0, 100, 180], range=["#f3f1ec", "#e8a33d", "#b2301e"])),
        tooltip=[alt.Tooltip("week_start:T", title="week"), "employee", alt.Tooltip("util:Q", format=".0f", title="utilization %"),
                 "ActiveEvents"]).properties(height=260))
    t = read("v_turned_away_demand").groupby("requested_month", as_index=False).inquiries_turned_away.sum()
    st.subheader("Couples turned away (date unavailable), by requested wedding month")
    st.bar_chart(t, x="requested_month", y="inquiries_turned_away", color="#b2301e", height=240)

with p2:
    f = read("v_funnel_by_channel").sort_values("inquiries", ascending=False)
    a, b, c = st.columns(3)
    decided = f.inquiries.sum() - f.still_open.sum()
    a.metric("Inquiries", int(f.inquiries.sum()))
    b.metric("Conversion (decided)", f"{f.booked.sum() / decided:.1%}")
    nr = read("v_no_reply_rate")
    c.metric("No-reply rate", f"{nr.no_reply.sum() / nr.decided.sum():.0%}", help="Closed by the 30-day silence rule, of decided inquiries")
    left, right = st.columns(2)
    left.subheader("Funnel by channel")
    left.dataframe(f.rename(columns={"conversion_rate_decided": "conversion"}), hide_index=True, width="stretch")
    m = nr.groupby("inquiry_month", as_index=False)[["decided", "no_reply"]].sum()
    m["no_reply_rate"] = m.no_reply / m.decided
    right.subheader("No-reply rate by inquiry month")
    right.line_chart(m, x="inquiry_month", y="no_reply_rate", color="#b2301e", height=300)
    left, right = st.columns(2)
    left.subheader("Reply speed vs conversion")
    left.bar_chart(read("v_response_vs_conversion"), x="ResponseTimeBand", y="conversion_rate", height=260)
    right.subheader("Why we lose couples")
    right.bar_chart(read("v_lost_reasons"), x="LostReason", y="lost", height=260)

with follow:
    fl = read("v_followup_list")
    if fl.empty:
        st.info("No scores yet: run the daily model steps (python -m ml daily).")
    else:
        st.caption(f"Open inquiries on {fl.AsOfDate.iloc[0]}, scored by model v{fl.Version.iloc[0]}. "
                   "Call the High band first; a long silence means the couple is about to drop out.")
        fl = fl.sort_values("Score", ascending=False)
        fl["Score"] = fl.Score.astype(float)
        st.dataframe(fl.drop(columns=["AsOfDate", "Version"]), hide_index=True, width="stretch",
                     column_config={"Score": st.column_config.ProgressColumn("Chance to book", min_value=0, max_value=1, format="%.2f"),
                                    "HoursSilent": st.column_config.NumberColumn("Hours since couple wrote", format="%.0f")})

with health:
    h = read("v_model_health")
    if h.empty:
        st.info("No production model yet: run python -m ml train.")
    else:
        r = h.iloc[0]
        a, b, c = st.columns(3)
        a.metric("Production model", f"v{r.Version} · {r.Algorithm}")
        b.metric("Test AUC", f"{float(r.test_auc):.2f}", f"{float(r.test_auc) - float(r.baseline_auc):+.2f} vs baseline")
        c.metric("Trained on data to", str(r.trained_on_data_to))
        if r.checked_on is not None:
            st.subheader(f"Monitoring on {r.checked_on}")
            st.dataframe(h[["Check", "Value", "Status", "Detail"]], hide_index=True, width="stretch")
    st.subheader("Model registry")
    st.dataframe(read("v_model_versions").sort_values("Version", ascending=False), hide_index=True, width="stretch")
