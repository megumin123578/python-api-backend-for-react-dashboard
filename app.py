
import io
import os
import re
import base64
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from datetime import datetime, date

st.set_page_config(page_title="YouTube CSV Dashboard", layout="wide")

# ------------------------------
# Helpers
# ------------------------------

@st.cache_data(show_spinner=False)
def parse_csv(file_bytes, filename):
    # Try utf-8-sig first, then fallback to cp1252
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            df = pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
            break
        except Exception:
            df = None
    if df is None:
        raise ValueError(f"Cannot read CSV: {filename}")
    df.columns = [c.strip() for c in df.columns]
    df = normalize_columns(df, filename)
    return df

def normalize_columns(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Map various possible column names to a standard schema."""
    orig_cols = {c.lower(): c for c in df.columns}

    def pick(*cands):
        for c in cands:
            if c in orig_cols:
                return orig_cols[c]
        return None

    # Guess channel name
    channel_col = pick("channel", "channel_name", "channel title", "channeltitle", "channel_title")
    if channel_col is None:
        # derive from filename (stem without extension)
        stem = os.path.splitext(os.path.basename(filename))[0]
        df["channel"] = stem
    else:
        df = df.rename(columns={channel_col: "channel"})

    # Date
    day_col = pick("date", "day", "time", "day_date")
    if day_col is not None:
        df = df.rename(columns={day_col: "date"})
        # Coerce to datetime
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    else:
        # No date present; create a fake single date to allow grouping
        df["date"] = pd.NaT

    # Standard numeric columns mapping
    mappings = {
        "views": ["views", "view"],
        "watch_time_minutes": [
            "watch time (minutes)", "watch_time_minutes",
            "estimatedminuteswatched", "estimated_minutes_watched", "watchtime"
        ],
        "impressions": ["impressions", "adimpressions"],
        "ctr": ["ctr", "impressions click-through rate", "impressions_click_through_rate", "impressions_ctr"],
        "subscribers_gained": ["subscribersgained", "subscribers gained", "subs_gained", "subs gained"],
        "subscribers_lost": ["subscriberslost", "subscribers lost", "subs_lost", "subs lost"],
        "estimated_revenue": ["estimatedrevenue", "estimated revenue", "revenue"],
        "video_title": ["video title", "title", "videotitle"],
        "video_id": ["video id", "video_id", "id"],
        "traffic_source": ["trafficsource", "traffic source", "insightTrafficSourceType", "traffic_source_type"],
        "country": ["country", "geography", "region"]
    }

    for std, cands in mappings.items():
        for cand in cands:
            if cand.lower() in orig_cols:
                df = df.rename(columns={orig_cols[cand.lower()]: std})
                break
        if std not in df.columns:
            df[std] = np.nan

    # Ensure numeric types
    numeric_cols = [
        "views", "watch_time_minutes", "impressions", "ctr",
        "subscribers_gained", "subscribers_lost", "estimated_revenue"
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Clean strings
    for c in ["video_title", "video_id", "traffic_source", "country", "channel"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df

def format_int(n):
    if pd.isna(n):
        return "—"
    try:
        return f"{int(n):,}"
    except Exception:
        return f"{n}"

def format_float(n, decimals=2):
    if pd.isna(n):
        return "—"
    try:
        return f"{float(n):,.{decimals}f}"
    except Exception:
        return f"{n}"

def df_to_download_link(df, filename="filtered.csv"):
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    b64 = base64.b64encode(csv_bytes).decode()
    href = f'<a href="data:text/csv;base64,{b64}" download="{filename}">Download CSV</a>'
    return href

# ------------------------------
# Sidebar - Data input
# ------------------------------
st.sidebar.title("Data Source")
uploaded = st.sidebar.file_uploader(
    "Upload 1 hoặc nhiều CSV (mỗi kênh hoặc nhiều report)",
    type=["csv"],
    accept_multiple_files=True
)

if not uploaded:
    st.sidebar.info("Chưa có dữ liệu. Hãy upload CSV.")
    st.title("YouTube CSV Dashboard")
    st.write("👋 Upload các file CSV (YouTube Analytics/Data API export) để xem báo cáo.")
    st.stop()

dfs = []
for file in uploaded:
    try:
        data = file.read()
        df = parse_csv(data, file.name)
        dfs.append(df)
    except Exception as e:
        st.sidebar.error(f"{file.name}: {e}")

if not dfs:
    st.error("Không đọc được CSV nào.")
    st.stop()

data = pd.concat(dfs, ignore_index=True)
if data["date"].isna().all():
    # If no date in any file, set a default date (today) to allow filters/plots
    data["date"] = date.today()

# ------------------------------
# Sidebar - Filters
# ------------------------------
min_date = data["date"].min()
max_date = data["date"].max()
if pd.isna(min_date) or pd.isna(max_date):
    min_date = date.today()
    max_date = date.today()

st.sidebar.markdown("---")
date_range = st.sidebar.date_input(
    "Khoảng ngày",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

all_channels = sorted(data["channel"].dropna().unique().tolist())
sel_channels = st.sidebar.multiselect(
    "Kênh (multi-select)",
    options=all_channels,
    default=all_channels
)

# Optional dimensions if present
has_country = data["country"].notna().any()
has_source = data["traffic_source"].notna().any()

# Apply filters
d1, d2 = date_range if isinstance(date_range, (list, tuple)) else (min_date, max_date)
mask = (data["date"] >= d1) & (data["date"] <= d2) & (data["channel"].isin(sel_channels))
f = data.loc[mask].copy()

# ------------------------------
# KPIs
# ------------------------------
total_views = f["views"].sum()
total_watch = f["watch_time_minutes"].sum()
total_impr = f["impressions"].sum()
avg_ctr = f["ctr"].replace([np.inf, -np.inf], np.nan).mean()
subs_gain = f["subscribers_gained"].sum()
subs_lost = f["subscribers_lost"].sum()
revenue = f["estimated_revenue"].sum()

st.title("📊 YouTube CSV Dashboard")
kpi_cols = st.columns(6)
kpi_cols[0].metric("Views", format_int(total_views))
kpi_cols[1].metric("Watch time (min)", format_int(total_watch))
kpi_cols[2].metric("Impressions", format_int(total_impr))
kpi_cols[3].metric("Avg CTR (%)", format_float(avg_ctr * 100 if pd.notna(avg_ctr) else np.nan, 2))
kpi_cols[4].metric("Subs +", format_int(subs_gain))
kpi_cols[5].metric("Subs -", format_int(subs_lost))

# ------------------------------
# Charts
# ------------------------------
st.markdown("### 📈 Xu hướng theo ngày")
if not f.empty:
    # daily by channel
    by_day = f.groupby(["date", "channel"], as_index=False)[["views", "watch_time_minutes", "impressions"]].sum()
    tab1, tab2, tab3 = st.tabs(["Views", "Watch time", "Impressions"])

    with tab1:
        fig = px.line(by_day, x="date", y="views", color="channel", markers=True, title="Views by Day")
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig = px.line(by_day, x="date", y="watch_time_minutes", color="channel", markers=True, title="Watch Time (min) by Day")
        st.plotly_chart(fig, use_container_width=True)
    with tab3:
        fig = px.line(by_day, x="date", y="impressions", color="channel", markers=True, title="Impressions by Day")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Không có dữ liệu trong khoảng lọc.")

# ------------------------------
# Breakdown by Traffic Source / Country
# ------------------------------
cols = st.columns(2)

with cols[0]:
    st.markdown("### 🛣️ Traffic Sources")
    if has_source and not f["traffic_source"].dropna().empty:
        src = f.groupby(["traffic_source", "channel"], as_index=False)["views"].sum()
        fig = px.bar(src, x="traffic_source", y="views", color="channel", title="Views by Traffic Source", barmode="group")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Không có cột 'traffic_source'.")

with cols[1]:
    st.markdown("### 🌍 Country")
    if has_country and not f["country"].dropna().empty:
        geo = f.groupby(["country", "channel"], as_index=False)["views"].sum()
        fig = px.bar(geo, x="country", y="views", color="channel", title="Views by Country", barmode="group")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Không có cột 'country'.")

# ------------------------------
# Top Videos
# ------------------------------
st.markdown("### ⭐ Top videos (by Views)")
group_keys = ["channel"] + (["video_id"] if "video_id" in f.columns else ["video_title"])
top = f.groupby(group_keys, as_index=False).agg({
    "views": "sum",
    "watch_time_minutes": "sum",
    "impressions": "sum",
    "ctr": "mean"
}).sort_values("views", ascending=False).head(50)
st.dataframe(top, use_container_width=True)
st.markdown(df_to_download_link(top, filename="top_videos.csv"), unsafe_allow_html=True)

# ------------------------------
# Raw Table + Export
# ------------------------------
with st.expander("🔎 Dữ liệu đã lọc (raw)"):
    st.dataframe(f, use_container_width=True, height=400)
    st.markdown(df_to_download_link(f, filename="filtered_data.csv"), unsafe_allow_html=True)

# ------------------------------
# Notes
# ------------------------------
with st.expander("ℹ️ Hướng dẫn & Gợi ý CSV"):
    st.write("""
**Cột được hỗ trợ (không bắt buộc đầy đủ, tự map tên gần đúng):**  
- `date` / `day`  
- `channel` / `channel_title`  
- `views`, `watch_time_minutes`, `impressions`, `ctr`  
- `subscribers_gained`, `subscribers_lost`, `estimated_revenue`  
- `video_title`, `video_id`  
- `traffic_source`, `country`

**Mẹo:**  
- Gộp nhiều CSV bằng cách upload **nhiều file**; app tự thêm cột `channel` từ file hoặc từ cột trong CSV.  
- Nếu concat nhiều nguồn khác nhau, app sẽ tự cố gắng chuẩn hóa tên cột.  
- Nếu không có `date`, app sẽ tạm dùng ngày hiện tại để có thể vẽ biểu đồ.
""")
