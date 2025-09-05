# routes/traffic_timeseries.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import date
from sqlalchemy import text
from db import engine

def query_all_safe(sql: str, params=None):
    try:
        with engine.begin() as conn:
            rs = conn.execute(text(sql), params or {})
            # SQLAlchemy 2.x: dùng .mappings().all() để lấy RowMapping
            return rs.mappings().all()
    except Exception as e:
        # Không cho nổ API /channels; log ra và trả về rỗng để dùng fallback
        print("[query_all_safe] failed:", e)
        return []

router = APIRouter(prefix="/api/traffic_source", tags=["traffic_source"])

def resolve_channel(channel_root: str):
    """
    Hỗ trợ 2 dạng:
    - "xgaming"                  -> account_tag='xgaming', channel_id=None (KHÔNG lọc channel_id)
    - "xgaming__UCabc123"        -> account_tag='xgaming', channel_id='UCabc123'
    """
    if "__" in channel_root:
        account_tag, channel_id = channel_root.split("__", 1)
        channel_id = channel_id.strip() or None
    else:
        account_tag, channel_id = channel_root.strip(), None
    return {"account_tag": account_tag, "channel_id": channel_id}

# ========= NEW: Lấy danh sách channel cho dropdown =========
@router.get("/channels")
def list_channels():
    rows_acc = query_all_safe("""
        SELECT DISTINCT
            account_tag,
            account_tag AS label
        FROM traffic_source_daily
        WHERE account_tag IS NOT NULL AND account_tag <> ''
        ORDER BY 2;
    """)
    items = [{"value": r["account_tag"], "label": r["account_tag"]} for r in rows_acc]
    return {"items": items}



class TSRequest(BaseModel):
    start: date
    end: date
    channelRoot: str
    interval: str  # daily | weekly | monthly | yearly

@router.post("/timeseries")
def timeseries(req: TSRequest):
    interval_map = {"daily": "day", "weekly": "week", "monthly": "month", "yearly": "year"}
    if req.interval not in interval_map:
        raise HTTPException(400, "interval phải là daily/weekly/monthly/yearly")

    ch = resolve_channel(req.channelRoot)
    cond_channel = "AND channel_id = :channel_id" if ch["channel_id"] is not None else ""

    sql = text(f"""
        SELECT
          date_trunc(:bucket, day)::date AS bucket,
          source,
          SUM(views)::bigint AS "views",
          SUM(estimated_minutes_watched)::bigint AS "estimatedMinutesWatched",
          SUM(engaged_views)::bigint AS "engagedViews",
          CASE WHEN SUM(views) > 0
               THEN SUM(average_view_duration * views)::float / SUM(views)
               ELSE 0 END AS "averageViewDuration",
          CASE WHEN SUM(views) > 0
               THEN SUM(average_view_percentage * views)::float / SUM(views)
               ELSE 0 END AS "averageViewPercentage"
        FROM traffic_source_daily
        WHERE account_tag = :account_tag
          {cond_channel}
          AND day BETWEEN :start AND :end
        GROUP BY bucket, source
        ORDER BY bucket ASC, source ASC
    """)

    params = {
        "bucket": interval_map[req.interval],
        "account_tag": ch["account_tag"],
        "start": req.start,
        "end": req.end,
    }
    if ch["channel_id"] is not None:
        params["channel_id"] = ch["channel_id"]

    with engine.begin() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return rows


class RangeRequest(BaseModel):
    start: date
    end: date
    channelRoot: str

@router.post("/range")
def range_aggregate(req: RangeRequest):
    ch = resolve_channel(req.channelRoot)
    cond_channel = "AND channel_id = :channel_id" if ch["channel_id"] is not None else ""

    sql = text(f"""
        SELECT
          source,
          SUM(views)::bigint AS "views",
          SUM(estimated_minutes_watched)::bigint AS "estimatedMinutesWatched",
          SUM(engaged_views)::bigint AS "engagedViews",
          CASE WHEN SUM(views) > 0
               THEN SUM(average_view_duration * views)::float / SUM(views)
               ELSE 0 END AS "averageViewDuration",
          CASE WHEN SUM(views) > 0
               THEN SUM(average_view_percentage * views)::float / SUM(views)
               ELSE 0 END AS "averageViewPercentage"
        FROM traffic_source_daily
        WHERE account_tag = :account_tag
          {cond_channel}
          AND day BETWEEN :start AND :end
        GROUP BY source
        ORDER BY "views" DESC
    """)

    params = {
        "account_tag": ch["account_tag"],
        "start": req.start,
        "end": req.end,
    }
    if ch["channel_id"] is not None:
        params["channel_id"] = ch["channel_id"]

    with engine.begin() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return [{"id": r["source"], "label": r["source"], **r} for r in rows]
