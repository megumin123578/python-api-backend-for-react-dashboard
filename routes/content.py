# routes/content.py
import os
from fastapi import APIRouter
from pydantic import BaseModel
from datetime import date
from sqlalchemy import text
from db import engine
from module_trafficsource import sanitize_filename  # dùng lại hàm này

router = APIRouter(prefix="/api/content", tags=["content"])

CREDENTIALS_DIR = "./credentials"


# ==============================
# Helper query
# ==============================
def query_all_safe(sql: str, params=None):
    try:
        with engine.begin() as conn:
            rs = conn.execute(text(sql), params or {})
            return rs.mappings().all()
    except Exception as e:
        print("[content.query_all_safe] failed:", e)
        return []


@router.get("/channels")
def list_channels():
    items = []
    try:
        for fname in os.listdir(CREDENTIALS_DIR):
            if not fname.endswith(".json"):
                continue

            raw = fname[:-5]               # bỏ .json
            value = sanitize_filename(raw) 
            items.append({
                "value": value,  
                "label": raw,   
            })
    except Exception as e:
        print("[content.channels] ERROR:", e)

    return {"items": items}


class ContentListRequest(BaseModel):
    start: date
    end: date
    channelId: str


@router.post("/list")
def content_list(req: ContentListRequest):
    sql = """
        SELECT
        v.video_id      AS "videoId",
        v.title,
        v.thumbnail,
        v.published_at  AS "publishedAt",
        v.duration,

        -- Dùng views từ metadata → CHUẨN NHẤT
        v.views AS views,

        -- Watch time vẫn SUM theo daily stats
        COALESCE(SUM(s.estimated_minutes) / 60.0, 0) AS "watchTimeHours",

        0::bigint  AS "subscribers",
        0::numeric AS "estimatedRevenue",
        0::bigint  AS "impressions",
        0::numeric AS "ctr"
    FROM videos v
    LEFT JOIN video_daily_stats s
    ON s.video_id = v.video_id
    AND s.day BETWEEN :start AND :end
    WHERE v.account_tag = :account_tag
    GROUP BY
        v.video_id,
        v.title,
        v.thumbnail,
        v.published_at,
        v.duration,
        v.views      -- thêm vào GROUP BY
    ORDER BY v.published_at DESC;

"""


    params = {
        "start": req.start,
        "end": req.end,
        "account_tag": req.channelId,
    }

    rows = query_all_safe(sql, params)
    # print("[content.list] rows =", rows[:3])  # debug 
    return {"items": rows}


class TimeSeriesRequest(BaseModel):
    start: date
    end: date
    channelId: str  # = account_tag


@router.post("/timeseries")
def content_timeseries(req: TimeSeriesRequest):
    """
    Trả timeseries theo từng video, dùng bảng video_daily_stats.

    Response mẫu:
    {
      "items": [
        {
          "bucket": "2025-10-13",
          "videoId": "ZNBpaS_-epw",
          "title": "yummy",
          "views": 627,
          "watch_hours": 1.23,
          "subs": 0,
          "revenue": 0,
          "impressions": 0
        },
        ...
      ]
    }
    """

    sql = """
        SELECT
            s.day                  AS bucket,
            v.video_id             AS "videoId",
            v.title                AS title,

            s.views                AS views,
            (s.estimated_minutes / 60.0) AS watch_hours,

            0::bigint              AS subs,
            0::numeric             AS revenue,
            0::bigint              AS impressions
        FROM video_daily_stats s
        JOIN videos v
          ON v.video_id = s.video_id
        WHERE v.account_tag = :account_tag
          AND s.day BETWEEN :start AND :end
        ORDER BY
            bucket ASC,
            "videoId" ASC;
    """

    params = {
        "account_tag": req.channelId,
        "start": req.start,
        "end": req.end,
    }

    rows = query_all_safe(sql, params)
    # print("[content.timeseries] rows (sample) =", rows[:5])  # debug
    return {"items": rows}
