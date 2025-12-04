# routes/overview.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import date
from sqlalchemy import text
from db import engine
from typing import Optional

router = APIRouter(prefix="/api/video_overview", tags=["video_overview"])


# ---------------------------------------------------------------------
# Utility query
# ---------------------------------------------------------------------
def query(sql: str, params=None):
    try:
        with engine.begin() as conn:
            rs = conn.execute(text(sql), params or {})
            return rs.mappings().all()
    except Exception as e:
        print("[DB ERROR]", e)
        return []


# ---------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------
class VideoFilter(BaseModel):
    accountTag: str
    startDate: Optional[date] = None
    endDate: Optional[date] = None


# ---------------------------------------------------------------------
# 1) LẤY DANH SÁCH ACCOUNT
# ---------------------------------------------------------------------
@router.get("/channels")
def list_channels():
    rows = query("""
        SELECT DISTINCT account_tag AS value, account_tag AS label
        FROM video_overview
        WHERE account_tag IS NOT NULL
        ORDER BY account_tag;
    """)

    return {"items": rows}


# ---------------------------------------------------------------------
# 2) LẤY DANH SÁCH VIDEO THEO ACCOUNT_TAG
# ---------------------------------------------------------------------
@router.get("/videos")
def list_videos(accountTag: str):
    rows = query("""
        SELECT
            account_tag,
            video_id,
            title,
            thumbnail,
            publish_date,
            views,
            likes,
            comments,
            dislikes,
            engaged_views,
            annotation_click_through_rate,
            annotation_close_rate,
            average_view_duration_seconds,
            shares,
            subscribers_gained,
            subscribers_lost,
            updated_at
        FROM video_overview
        WHERE account_tag = :tag
        ORDER BY publish_date DESC;
    """, {"tag": accountTag})

    return rows


# ---------------------------------------------------------------------
# 3) LỌC VIDEO THEO NGÀY ĐĂNG
# ---------------------------------------------------------------------
@router.post("/list")
def list_filtered(req: VideoFilter):
    sql = """
        SELECT
            video_id, title, thumbnail, publish_date,
            views, likes, comments,
            dislikes, engaged_views,
            annotation_click_through_rate,
            annotation_close_rate,
            average_view_duration_seconds,
            shares, subscribers_gained, subscribers_lost,
            updated_at
        FROM video_overview
        WHERE account_tag = :tag
    """

    params = {"tag": req.accountTag}

    if req.startDate:
        sql += " AND publish_date >= :startDate"
        params["startDate"] = req.startDate

    if req.endDate:
        sql += " AND publish_date <= :endDate"
        params["endDate"] = req.endDate

    sql += " ORDER BY publish_date DESC"

    rows = query(sql, params)
    return rows


# ---------------------------------------------------------------------
# 4) XEM CHI TIẾT 1 VIDEO
# ---------------------------------------------------------------------
@router.get("/detail/{video_id}")
def video_detail(video_id: str):
    rows = query("""
        SELECT *
        FROM video_overview
        WHERE video_id = :vid
        LIMIT 1;
    """, {"vid": video_id})

    if not rows:
        raise HTTPException(404, "Video not found")

    return rows[0]


# ---------------------------------------------------------------------
# 5) AGGREGATE (views, likes, comments, subs…)
# ---------------------------------------------------------------------
class AggRequest(BaseModel):
    accountTag: str
    start: date
    end: date


@router.post("/stats")
def overview_stats(req: AggRequest):
    rows = query("""
        SELECT
            COUNT(*) AS totalVideos,
            SUM(views)::bigint AS views,
            SUM(likes)::bigint AS likes,
            SUM(comments)::bigint AS comments,
            SUM(dislikes)::bigint AS dislikes,
            SUM(engaged_views)::bigint AS engagedViews,
            SUM(subscribers_gained)::bigint AS subsGained,
            SUM(subscribers_lost)::bigint AS subsLost
        FROM video_overview
        WHERE account_tag = :tag
          AND publish_date BETWEEN :start AND :end
    """, {
        "tag": req.accountTag,
        "start": req.start,
        "end": req.end,
    })

    return rows[0] if rows else {}
