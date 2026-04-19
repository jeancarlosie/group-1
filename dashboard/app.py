import io
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone

# Configuration
ACCOUNT_NAME   = "iesstsabbadbab"
ACCOUNT_KEY    = "QOlT3Y2sp4wDkdW+Yb3Cddfo9PdoG8+ORBZy0ZDMZCiQtkK+rtI+Ri1WVXopfWgyoqC8N69czF9L+AStbdQ8bA=="
CONTAINER_NAME = "group01output"
ORDERS_PREFIX  = "orders/"
COURIER_PREFIX = "courier_state_events/"
ACCOUNT_URL    = f"https://{ACCOUNT_NAME}.blob.core.windows.net"

# Only read the N most-recently-written parquet files per topic (since Spark writes one file every 10s, 6 would equal the last minute of data.
MAX_FILES = 6

ZONE_ORDER = [f"zone_{i}" for i in range(5)]
ORDER_EVENTS  = ["ORDER_CREATED","RESTAURANT_ACCEPTED","PREP_STARTED",
                 "PREP_DONE","COURIER_ASSIGNED","PICKED_UP","DELIVERED","CANCELLED"]
COURIER_EVENTS = ["ONLINE","LOCATION_UPDATE","ARRIVED_RESTAURANT",
                  "PICKED_UP_ORDER","ARRIVED_CUSTOMER","OFFLINE"]
ORDER_COLORS = {
    "ORDER_CREATED":"#38BDF8","RESTAURANT_ACCEPTED":"#34D399",
    "PREP_STARTED":"#A78BFA","PREP_DONE":"#FBBF24",
    "COURIER_ASSIGNED":"#F472B6","PICKED_UP":"#FB923C",
    "DELIVERED":"#4ADE80","CANCELLED":"#F87171",
}

# Page
st.set_page_config(page_title="Group 1 — Live Delivery Dashboard", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@700&family=DM+Sans:wght@400;600&display=swap');
html, body, [class*="css"] {
    background:#0A0F1E !important; color:#E2E8F0 !important;
    font-family:'DM Sans',sans-serif;
}
.main .block-container { padding:1.5rem 2rem; max-width:1400px; }
.kpi { background:#111827; border:1px solid #1E293B; border-radius:10px;
       padding:1rem 1.3rem; }
.kpi-label { font-size:0.7rem; color:#64748B; text-transform:uppercase;
             letter-spacing:1.2px; font-family:'Space Mono',monospace; }
.kpi-value { font-family:'Space Mono',monospace; font-size:1.9rem;
             font-weight:700; color:#F8FAFC; line-height:1.1; }
.kpi-sub   { font-size:0.72rem; color:#475569; margin-top:3px; }
.dot { display:inline-block; width:8px; height:8px; border-radius:50%;
       background:#4ADE80; box-shadow:0 0 7px #4ADE80;
       animation:pulse 1.8s infinite; margin-right:6px; vertical-align:middle; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
.ts { font-size:0.75rem; color:#475569; font-family:'Space Mono',monospace;
      margin-bottom:1.2rem; }
#MainMenu,footer,header{visibility:hidden}
</style>
""", unsafe_allow_html=True)

# Data
def load_recent(prefix: str, max_files: int = MAX_FILES):
    """
    Read only the MAX_FILES most-recently-modified parquet blobs under prefix.
    This gives a rolling live window instead of an ever-growing cumulative dump.
    BlobServiceClient is NOT cached so every call gets a fresh blob listing.
    """
    try:
        container = BlobServiceClient(
            account_url=ACCOUNT_URL, credential=ACCOUNT_KEY
        ).get_container_client(CONTAINER_NAME)

        blobs = sorted(
            [b for b in container.list_blobs(name_starts_with=prefix)
             if b.name.endswith(".parquet")],
            key=lambda x: x.last_modified,
            reverse=True,
        )
    except Exception as e:
        st.warning(f"Azure error: {e}")
        return pd.DataFrame(), None

    if not blobs:
        return pd.DataFrame(), None

    newest_ts = blobs[0].last_modified
    recent    = blobs[:max_files]

    dfs = []
    for b in recent:
        try:
            raw = container.download_blob(b.name).readall()
            dfs.append(pd.read_parquet(io.BytesIO(raw)))
        except Exception:
            pass

    if not dfs:
        return pd.DataFrame(), newest_ts

    df = pd.concat(dfs, ignore_index=True)

    if "event_time" in df.columns:
        col = df["event_time"]
        if pd.api.types.is_numeric_dtype(col):
            df["event_time"] = pd.to_datetime(col, unit="ms", errors="coerce")
        else:
            df["event_time"] = pd.to_datetime(col, errors="coerce")

    return df, newest_ts

# Helpers
def fmt(ts):
    if ts is None: return "—"
    try: return pd.Timestamp(ts).strftime("%H:%M:%S")
    except: return "—"

def active_couriers(df):
    if df.empty or "courier_id" not in df.columns: return 0
    if "event_type" not in df.columns: return df["courier_id"].nunique()
    latest = (df.dropna(subset=["event_time"])
               .sort_values("event_time")
               .groupby("courier_id", as_index=False).tail(1))
    return int(latest[latest["event_type"] != "OFFLINE"]["courier_id"].nunique())

DARK = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans", color="#94A3B8", size=12),
    margin=dict(l=10,r=10,t=36,b=10),
    xaxis=dict(gridcolor="#1E293B", zerolinecolor="#1E293B"),
    yaxis=dict(gridcolor="#1E293B", zerolinecolor="#1E293B"),
    title_font=dict(family="Space Mono", size=12, color="#CBD5E1"),
)

# Live Fragment
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=15_000, key="live_refresh")

def dashboard():
    orders,  o_ts  = load_recent(ORDERS_PREFIX)
    couriers, c_ts = load_recent(COURIER_PREFIX)

    now   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    o_blob = o_ts.strftime("%H:%M:%S") if o_ts else "—"
    c_blob = c_ts.strftime("%H:%M:%S") if c_ts else "—"

    st.markdown(
        f'<h1>🛵 Food Delivery &nbsp;<span style="font-size:0.9rem;color:#64748B">'
        f'<span class="dot"></span>LIVE</span></h1>',
        unsafe_allow_html=True
    )
    st.markdown(
        f'<div class="ts">Refreshes every 15 s &nbsp;·&nbsp; {now} '
        f'&nbsp;·&nbsp; latest orders blob: {o_blob} '
        f'&nbsp;·&nbsp; latest courier blob: {c_blob}</div>',
        unsafe_allow_html=True
    )

    if orders.empty and couriers.empty:
        st.warning("No data yet — Spark hasn't written any Parquet files. "
                   "Make sure the blob-write queries are running.")
        return

    # KPIs
    n_created   = int((orders["event_type"] == "ORDER_CREATED").sum())  if not orders.empty and "event_type" in orders.columns else 0
    n_delivered = int((orders["event_type"] == "DELIVERED").sum())      if not orders.empty and "event_type" in orders.columns else 0
    n_cancelled = int((orders["event_type"] == "CANCELLED").sum())      if not orders.empty and "event_type" in orders.columns else 0
    cancel_pct  = f"{100*n_cancelled/n_created:.1f}%" if n_created > 0 else "—"
    active_now  = active_couriers(couriers)
    latest_o    = fmt(orders["event_time"].max()   if not orders.empty  and "event_time" in orders.columns else None)
    latest_c    = fmt(couriers["event_time"].max() if not couriers.empty and "event_time" in couriers.columns else None)

    k1, k2, k3, k4, k5 = st.columns(5)
    for col, label, value, sub, accent in [
        (k1, "Orders created",  f"{n_created:,}",    f"latest @ {latest_o}",                        "#38BDF8"),
        (k2, "Delivered",       f"{n_delivered:,}",  f"{100*n_delivered/max(n_created,1):.0f}% of created", "#4ADE80"),
        (k3, "Cancelled",       f"{n_cancelled:,}",  f"cancel rate {cancel_pct}",                   "#F87171"),
        (k4, "Active couriers", f"{active_now}",     f"latest @ {latest_c}",                        "#FBBF24"),
        (k5, "Rolling window",  f"{MAX_FILES} files",f"~{MAX_FILES*10}s of live data",              "#A78BFA"),
    ]:
        col.markdown(
            f'<div class="kpi" style="border-top:3px solid {accent}">'
            f'<div class="kpi-label">{label}</div>'
            f'<div class="kpi-value">{value}</div>'
            f'<div class="kpi-sub">{sub}</div></div>',
            unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # Row 1: orders by zone, event mix donut
    c1, c2 = st.columns([1.5, 1])
    with c1:
        if not orders.empty and "zone_id" in orders.columns:
            zc = (orders.groupby("zone_id").size()
                  .reindex(ZONE_ORDER, fill_value=0).reset_index(name="n"))
            fig = px.bar(zc, x="zone_id", y="n", title="Orders by zone",
                         category_orders={"zone_id": ZONE_ORDER})
            fig.update_traces(marker_color="#38BDF8", marker_line_width=0)
            fig.update_layout(**DARK)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})

    with c2:
        if not orders.empty and "event_type" in orders.columns:
            em = orders.groupby("event_type").size().reset_index(name="n")
            fig = px.pie(em, names="event_type", values="n", hole=0.45,
                         title="Event mix",
                         color="event_type", color_discrete_map=ORDER_COLORS)
            fig.update_traces(textposition="outside", textinfo="label+percent",
                              textfont_size=10)
            fig.update_layout(**DARK)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})

    # Row 2: delivery funnel, avg order value
    c3, c4 = st.columns(2)
    with c3:
        if not orders.empty and {"event_type", "order_id"}.issubset(orders.columns):
            stages = [
                "ORDER_CREATED",
                "RESTAURANT_ACCEPTED",
                "PREP_STARTED",
                "PREP_DONE",
                "COURIER_ASSIGNED",
                "PICKED_UP",
                "DELIVERED",
            ]

            stage_df = (
                orders[orders["event_type"].isin(stages)][["order_id", "event_type"]]
                .dropna()
                .drop_duplicates(["order_id", "event_type"])
            )

            counts = (
                stage_df.groupby("event_type")["order_id"]
                .nunique()
                .reindex(stages, fill_value=0)
            )

            pairs = [(stage, int(counts.loc[stage])) for stage in stages if counts.loc[stage] > 0]

            if pairs:
                fig = go.Figure(go.Funnel(
                    y=[x[0] for x in pairs],
                    x=[x[1] for x in pairs],
                    textinfo="value+percent initial",
                    marker=dict(
                        color=["#38BDF8", "#34D399", "#A78BFA",
                              "#FBBF24", "#F472B6", "#FB923C", "#4ADE80"][:len(pairs)],
                        line=dict(width=0)
                    ),
                ))
                fig.update_layout(
                    title="Order lifecycle funnel (unique orders)",
                    **{**DARK, "margin": dict(l=160, r=10, t=36, b=10)}
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    with c4:
        if not orders.empty and "order_value" in orders.columns and "event_type" in orders.columns:
            sub = orders[orders["event_type"]=="ORDER_CREATED"].dropna(subset=["order_value"])
            if not sub.empty:
                agg = (sub.groupby("zone_id")["order_value"]
                       .agg(avg="mean", min="min", max="max")
                       .reset_index().sort_values("zone_id"))
                fig = go.Figure()
                for name, col_key, color in [("Avg","avg","#FBBF24"),
                                              ("Min","min","#38BDF8"),
                                              ("Max","max","#F472B6")]:
                    fig.add_trace(go.Bar(name=f"{name} €", x=agg["zone_id"],
                                         y=agg[col_key].round(2),
                                         marker_color=color, marker_line_width=0))
                fig.update_layout(barmode="group", title="Order value by zone (€)", **DARK)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})

    # Row 3: courier events, courier heatmap
    c5, c6 = st.columns([1, 1.5])
    with c5:
        if not couriers.empty and "event_type" in couriers.columns:
            ce = (couriers.groupby("event_type").size()
                  .reindex(COURIER_EVENTS, fill_value=0).reset_index(name="n"))
            fig = px.bar(ce, x="event_type", y="n", title="Courier event counts",
                         category_orders={"event_type": COURIER_EVENTS})
            fig.update_traces(marker_color="#34D399", marker_line_width=0)
            fig.update_layout(**DARK)
            fig.update_xaxes(tickangle=-30)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})

    with c6:
        if not couriers.empty and "zone_id" in couriers.columns and "event_type" in couriers.columns:
            pivot = (couriers.groupby(["zone_id","event_type"]).size()
                     .reset_index(name="n")
                     .pivot(index="zone_id", columns="event_type", values="n")
                     .fillna(0))
            fig = go.Figure(go.Heatmap(
                z=pivot.values, x=pivot.columns.tolist(),
                y=pivot.index.tolist(), colorscale="Teal",
            ))
            fig.update_layout(title="Courier events: zone × type",
                              **{**DARK, "margin":dict(l=70,r=10,t=36,b=60)})
            fig.update_xaxes(tickangle=-30)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})

    # Raw tables
    with st.expander("Raw orders sample (newest 50)"):
        if not orders.empty:
            st.dataframe(orders.sort_values("event_time", ascending=False).head(50),
                         use_container_width=True)
    with st.expander("Raw courier sample (newest 50)"):
        if not couriers.empty:
            st.dataframe(couriers.sort_values("event_time", ascending=False).head(50),
                         use_container_width=True)

dashboard()
