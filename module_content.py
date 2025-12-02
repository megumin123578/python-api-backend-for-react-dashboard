# module_content_v3.py
# ---------------------------------------
# LẤY DỮ LIỆU VIDEO QUA YouTube Data API v3
# KHÔNG DÙNG Analytics API → KHÔNG LỖI 401
# ---------------------------------------

import os
from typing import List, Dict
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text

from module_trafficsource import (
    create_token_from_credentials,
    sanitize_filename
)


# ============================
# LẤY DANH SÁCH VIDEO
# ============================

def get_upload_playlist_id(credentials):
    yt = build("youtube", "v3", credentials=credentials)
    resp = yt.channels().list(
        part="contentDetails",
        mine=True
    ).execute()

    items = resp.get("items", [])
    if not items:
        return None

    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_video_list(credentials, playlist_id: str) -> List[str]:
    yt = build("youtube", "v3", credentials=credentials)
    video_ids = []

    req = yt.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )

    while req:
        resp = req.execute()

        for item in resp.get("items", []):
            vid = item["contentDetails"]["videoId"]
            video_ids.append(vid)

        req = yt.playlistItems().list_next(req, resp)

    return video_ids


# ============================
# LẤY VIDEO DETAIL + STATISTICS (v3)
# ============================

def get_video_details(credentials, video_ids: List[str]) -> List[Dict]:
    """
    Lấy video:
    - title
    - thumbnail
    - publishedAt
    - duration (ISO 8601)
    - statistics (views, likes, comments,...)
    """
    yt = build("youtube", "v3", credentials=credentials)
    results = []

    # API limit: 50 ids mỗi request
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]

        resp = yt.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(chunk)
        ).execute()

        for item in resp.get("items", []):
            stats = item.get("statistics", {})

            results.append({
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "thumbnail": item["snippet"]["thumbnails"]["medium"]["url"],
                "published_at": item["snippet"]["publishedAt"][:10],
                "duration": item["contentDetails"]["duration"],

                # Statistics
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
                "favorites": int(stats.get("favoriteCount", 0)),
            })

    return results


# ============================
# LƯU VÀO POSTGRESQL
# ============================

def save_content_v3_to_postgres(videos, pg_url: str):
    engine = create_engine(pg_url, future=True)

    with engine.begin() as conn:

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                thumbnail TEXT,
                published_at DATE,
                duration TEXT,

                views INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                favorites INTEGER DEFAULT 0,

                watch_time_hours NUMERIC DEFAULT 0,
                subscribers INTEGER DEFAULT 0,
                estimated_revenue NUMERIC DEFAULT 0,
                impressions INTEGER DEFAULT 0,
                ctr NUMERIC DEFAULT 0
            );
        """))

        for v in videos:
            conn.execute(text("""
                INSERT INTO videos
                    (video_id, title, thumbnail, published_at, duration,
                     views, likes, comments, favorites)
                VALUES
                    (:id, :title, :thumb, :pub, :duration,
                     :views, :likes, :comments, :favorites)
                ON CONFLICT(video_id)
                DO UPDATE SET
                    views = EXCLUDED.views,
                    likes = EXCLUDED.likes,
                    comments = EXCLUDED.comments,
                    favorites = EXCLUDED.favorites;
            """), {
                "id": v["video_id"],
                "title": v["title"],
                "thumb": v["thumbnail"],
                "pub": v["published_at"],
                "duration": v["duration"],
                "views": v["views"],
                "likes": v["likes"],
                "comments": v["comments"],
                "favorites": v["favorites"],
            })


# ============================
# MAIN RUNNER
# ============================

def run_content_v3(credentials, account_tag, pg_url):
    playlist_id = get_upload_playlist_id(credentials)
    if not playlist_id:
        print("Không tìm thấy uploads playlist.")
        return

    print("→ Fetching video list...")
    video_ids = get_video_list(credentials, playlist_id)

    print(f"→ Found {len(video_ids)} videos")

    print("→ Fetching video details (YouTube Data API v3)...")
    videos = get_video_details(credentials, video_ids)

    print("→ Saving into PostgreSQL...")
    save_content_v3_to_postgres(videos, pg_url)

    print("✔ DONE: Saved all video details using Data API v3")


# ============================
# ENTRY FOR get_data.py
# ============================

def process_content(cred_file: str):
    cred_path = os.path.join("credentials", cred_file)
    pg_url = os.getenv("PG_URL")

    credentials = create_token_from_credentials(cred_path)
    account_tag = sanitize_filename(os.path.splitext(cred_file)[0])

    run_content_v3(credentials, account_tag, pg_url)
