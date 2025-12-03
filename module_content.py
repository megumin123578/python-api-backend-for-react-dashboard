import os
from typing import List, Dict
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text

from module_trafficsource import (
    create_token_from_credentials,
    sanitize_filename
)



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
            video_ids.append(item["contentDetails"]["videoId"])

        req = yt.playlistItems().list_next(req, resp)

    return video_ids


# ============================
# VIDEO METADATA (DATA API)
# ============================

def get_video_metadata(credentials, video_ids: List[str]) -> List[Dict]:
    yt = build("youtube", "v3", credentials=credentials)
    results = []

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
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
            })

    return results


# ============================
# DAILY METRICS (ANALYTICS API)
# ============================

def get_video_daily_analytics(credentials, video_id: str,
                              start_date: str, end_date: str) -> List[Dict]:

    yta = build("youtubeAnalytics", "v2", credentials=credentials)

    q = {
        "ids": "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": "day",
        "filters": f"video=={video_id}",
        "metrics": ",".join([
            "views",
            "estimatedMinutesWatched",
            "averageViewDuration",
        ]),
        "sort": "day"
    }

    try:
        resp = yta.reports().query(**q).execute() or {}
    except Exception as e:
        print(f"[ERROR] Failed daily analytics for {video_id}: {e}")
        return []

    rows = resp.get("rows") or []
    if not rows:
        return []

    col = {c["name"]: i for i, c in enumerate(resp["columnHeaders"])}

    i_day = col["day"]
    i_views = col["views"]
    i_emw = col["estimatedMinutesWatched"]
    i_avd = col["averageViewDuration"]

    results = []
    for r in rows:
        results.append({
            "video_id": video_id,
            "day": r[i_day],
            "views": int(r[i_views]),
            "estimated_minutes": int(r[i_emw]),
            "average_view_duration": int(r[i_avd]),
        })

    return results

def save_metadata(videos, account_tag: str, pg_url: str):
    engine = create_engine(pg_url, future=True)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                account_tag TEXT NOT NULL,
                title TEXT,
                thumbnail TEXT,
                published_at DATE,
                duration TEXT,
                views INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0
            );
        """))

        for v in videos:
            conn.execute(text("""
                INSERT INTO videos
                    (video_id, account_tag, title, thumbnail,
                     published_at, duration, views, likes, comments)
                VALUES
                    (:id, :acct, :title, :thumb, :pub, :duration,
                     :views, :likes, :comments)
                ON CONFLICT(video_id)
                DO UPDATE SET
                    views = EXCLUDED.views,
                    likes = EXCLUDED.likes,
                    comments = EXCLUDED.comments;
            """), {
                "id": v["video_id"],
                "acct": account_tag,
                "title": v["title"],
                "thumb": v["thumbnail"],
                "pub": v["published_at"],
                "duration": v["duration"],
                "views": v["views"],
                "likes": v["likes"],
                "comments": v["comments"],
            })


def save_daily_stats(daily_rows, pg_url: str):
    engine = create_engine(pg_url, future=True)

    with engine.begin() as conn:

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS video_daily_stats (
                video_id TEXT NOT NULL,
                day DATE NOT NULL,
                views INTEGER,
                estimated_minutes INTEGER,
                average_view_duration INTEGER,
                PRIMARY KEY (video_id, day)
            );
        """))

        for r in daily_rows:
            conn.execute(text("""
                INSERT INTO video_daily_stats
                    (video_id, day, views, estimated_minutes, average_view_duration)
                VALUES
                    (:id, :day, :views, :emw, :avd)
                ON CONFLICT (video_id, day)
                DO UPDATE SET
                    views = EXCLUDED.views,
                    estimated_minutes = EXCLUDED.estimated_minutes,
                    average_view_duration = EXCLUDED.average_view_duration;
            """), {
                "id": r["video_id"],
                "day": r["day"],
                "views": r["views"],
                "emw": r["estimated_minutes"],
                "avd": r["average_view_duration"],
            })


# ============================
# RUNNER
# ============================

def run_content_v3_hybrid(credentials, account_tag, pg_url):
    playlist_id = get_upload_playlist_id(credentials)
    if not playlist_id:
        print("Không tìm thấy uploads playlist.")
        return

    print("→ Fetching video list...")
    video_ids = get_video_list(credentials, playlist_id)
    print(f"→ Found {len(video_ids)} videos")

    print("→ Fetching video metadata...")
    videos = get_video_metadata(credentials, video_ids)

    print("→ Saving metadata to PostgreSQL...")
    save_metadata(videos, account_tag, pg_url)

    print("→ Fetching DAILY analytics via YouTube Analytics API...")
    daily_rows = []

    for v in videos:
        video_id = v["video_id"]
        published = v["published_at"]

        d = get_video_daily_analytics(credentials, video_id, published, "2099-01-01")
        daily_rows.extend(d)

    print("→ Saving daily stats...")
    save_daily_stats(daily_rows, pg_url)

    print("✔ DONE: Metadata + DAILY stats saved successfully")


def process_content(cred_file: str):
    cred_path = os.path.join("credentials", cred_file)
    pg_url = os.getenv("PG_URL")

    credentials = create_token_from_credentials(cred_path)
    account_tag = sanitize_filename(os.path.splitext(cred_file)[0])

    run_content_v3_hybrid(credentials, account_tag, pg_url)
