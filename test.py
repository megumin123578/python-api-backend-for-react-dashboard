import os
import csv
import pickle
import re
from typing import Dict, Any, List
from functools import lru_cache
from datetime import datetime, timedelta

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

# =========================
# Paths / Settings
# =========================
CREDENTIALS_FOLDER = "credentials"
TOKEN_FOLDER = "token"
REPORT_ROOT = "reports"

# Set CONTENT_OWNER_ID (External ID) for Owner mode; leave empty for Channel mode
CONTENT_OWNER_ID = os.environ.get("CONTENT_OWNER_ID", "").strip()
IS_OWNER_MODE = bool(CONTENT_OWNER_ID)

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtubepartner-channel-audit",
    "https://www.googleapis.com/auth/youtubepartner",
]

# =========================
# Utils
# =========================
def sanitize_filename(s: str) -> str:
    s = s.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_\-\.]", "_", s)

def save_csv(path: str, headers: List[str], rows: List[List[Any]]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(headers)
        if rows:
            w.writerows(rows)

def get_date_range():
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    return start_date, end_date

def create_token_from_credentials(cred_path: str):
    os.makedirs(TOKEN_FOLDER, exist_ok=True)
    token_filename = os.path.splitext(os.path.basename(cred_path))[0] + ".pickle"
    token_path = os.path.join(TOKEN_FOLDER, token_filename)

    creds = None
    if os.path.exists(token_path):
        try:
            with open(token_path, "rb") as f:
                creds = pickle.load(f)
        except Exception:
            creds = None

    if not creds or not getattr(creds, "valid", False):
        if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)
    return creds

def get_youtube_data(credentials):
    try:
        youtube = build("youtube", "v3", credentials=credentials)
        resp = youtube.channels().list(part="snippet,contentDetails,statistics", mine=True).execute()
        items = resp.get("items", [])
        if items:
            it = items[0]
            return {
                "title": it.get("snippet", {}).get("title", ""),
                "channel_id": it.get("id", ""),
                "subs": it.get("statistics", {}).get("subscriberCount", "0"),
                "views": it.get("statistics", {}).get("viewCount", "0"),
                "videos": it.get("statistics", {}).get("videoCount", "0"),
                "uploads_playlist": it.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", ""),
            }
    except Exception:
        pass
    return None

# =========================
# Query helpers
# =========================
def yta_query(yta, **req):
    return yta.reports().query(**req).execute()

def value_list_from_first_column(resp):
    rows = resp.get("rows") or []
    if not rows or not resp.get("columnHeaders"):
        return []
    return sorted({r[0] for r in rows})

@lru_cache(maxsize=None)
def metric_supported(ids_val: str, start_date: str, end_date: str, metric_name: str, owner_mode: bool) -> bool:
    """Probe one simple query once. Cache by signature."""
    try:
        yta = build("youtubeAnalytics", "v2")
        base_req = {"ids": ids_val, "startDate": start_date, "endDate": end_date}
        if owner_mode:
            base_req["onBehalfOfContentOwner"] = CONTENT_OWNER_ID
        yta_query(yta, **{**base_req, "dimensions": "day", "metrics": metric_name})
        return True
    except Exception:
        return False

def countries_with_data(yta, base_req) -> set:
    try:
        resp = yta_query(yta, **{**base_req, "dimensions": "country", "metrics": "views"})
        return set(value_list_from_first_column(resp))
    except HttpError:
        return set()

def get_top_video_ids(yta, base_req, limit=50) -> List[str]:
    try:
        resp = yta_query(
            yta, **{**base_req, "dimensions": "video", "metrics": "views", "sort": "-views", "maxResults": limit}
        )
        rows = resp.get("rows") or []
        return [r[0] for r in rows]
    except HttpError:
        return []

# =========================
# Base Reports (always safe)
# =========================
def build_safe_reports():
    base_mets = ["views", "estimatedMinutesWatched"]
    avg_mets = ["averageViewDuration", "averageViewPercentage"]

    reports = [
        {"name": "Time by day", "dimensions": "day", "metrics": base_mets + avg_mets},
        {"name": "Subs by day", "dimensions": "day", "metrics": ["subscribersGained", "subscribersLost", "views"]},
        {"name": "Engagement by day", "dimensions": "day", "metrics": ["likes", "shares", "comments", "views"]},

        {"name": "Geography by country", "dimensions": "country", "metrics": base_mets + avg_mets},

        {"name": "Devices (deviceType)", "dimensions": "deviceType", "metrics": base_mets},
        {"name": "Operating system", "dimensions": "operatingSystem", "metrics": base_mets},

        {"name": "Traffic sources", "dimensions": "insightTrafficSourceType",
         "metrics": base_mets + avg_mets + ["engagedViews"]},

        {"name": "Playback locations", "dimensions": "insightPlaybackLocationType",
         "metrics": base_mets + avg_mets + ["engagedViews"]},

        {"name": "Subscribed status", "dimensions": "subscribedStatus", "metrics": base_mets},
        {"name": "Live or OnDemand", "dimensions": "liveOrOnDemand", "metrics": base_mets},

        {"name": "Demographics (ageGroup,gender)", "dimensions": "ageGroup,gender",
         "metrics": ["viewerPercentage"], "params": {"sort": "-viewerPercentage"}},

        {"name": "Top videos", "dimensions": "video",
         "metrics": ["views", "estimatedMinutesWatched", "averageViewDuration", "engagedViews"],
         "params": {"sort": "-views", "maxResults": 200}},

        {"name": "Top playlists", "dimensions": "playlist",
         "metrics": base_mets, "params": {"sort": "-views", "maxResults": 200}},

        {"name": "Cards by day", "dimensions": "day",
         "metrics": ["cardImpressions", "cardClicks", "cardClickRate",
                     "cardTeaserImpressions", "cardTeaserClicks", "cardTeaserClickRate"]},

        {"name": "Annotations by day", "dimensions": "day",
         "metrics": ["annotationImpressions", "annotationClickableImpressions",
                     "annotationClicks", "annotationClickThroughRate",
                     "annotationClosableImpressions", "annotationCloses", "annotationCloseRate"]},
    ]

    # Owner-only dimensions (avoid 400 in channel mode)
    if IS_OWNER_MODE:
        reports += [
            {"name": "Uploader type", "dimensions": "uploaderType", "metrics": base_mets},
            {"name": "Sharing service", "dimensions": "sharingService", "metrics": base_mets},
            {"name": "Subtitle language", "dimensions": "subtitleLanguage", "metrics": base_mets},
            {"name": "Owner revenue by day", "dimensions": "day",
             "metrics": ["estimatedRevenue", "estimatedAdRevenue", "grossRevenue",
                         "cpm", "playbackBasedCpm", "adImpressions", "monetizedPlaybacks"]},
            {"name": "Owner revenue by country", "dimensions": "country",
             "metrics": ["estimatedRevenue", "estimatedAdRevenue", "grossRevenue",
                         "cpm", "playbackBasedCpm", "adImpressions", "monetizedPlaybacks"]},
            {"name": "Ad type", "dimensions": "adType",
             "metrics": ["adImpressions", "monetizedPlaybacks"]},
            {"name": "Claimed status", "dimensions": "claimedStatus",
             "metrics": ["views", "estimatedMinutesWatched"]},
            {"name": "By asset", "dimensions": "asset",
             "metrics": ["views", "estimatedMinutesWatched"]},
        ]

    return reports

# =========================
# Conditional Parts
# =========================
def run_province_reports_if_any(yta, base_req, out_dir):
    have = countries_with_data(yta, base_req)
    for cc in ("US", "CA"):
        if cc in have:
            try:
                resp = yta_query(yta, **{**base_req,
                                         "dimensions": "province",
                                         "metrics": "views,estimatedMinutesWatched",
                                         "filters": f"country=={cc}"})
                out = os.path.join(out_dir, f"Geography_by_province__{cc}.csv")
                headers = [h["name"] for h in resp.get("columnHeaders", [])]
                rows = resp.get("rows", [])
                save_csv(out, headers, rows)
                print(f"  ✓ Geography_by_province__{cc}: {len(rows)} rows -> {out}")
            except HttpError:
                print(f"  · Geography_by_province__{cc}: unsupported → skip")
        else:
            print(f"  · Geography_by_province__{cc}: no country traffic → skip")

def run_retention_per_video(yta, base_req, top_video_ids, out_dir):
    for vid in top_video_ids:
        try:
            resp = yta_query(yta, **{**base_req,
                                     "dimensions": "elapsedVideoTimeRatio",
                                     "metrics": "audienceWatchRatio",
                                     "filters": f"video=={vid};audienceType==ORGANIC"})
            out = os.path.join(out_dir, f"Audience_retention__{sanitize_filename(vid)}.csv")
            headers = [h["name"] for h in resp.get("columnHeaders", [])]
            rows = resp.get("rows", [])
            save_csv(out, headers, rows)
            print(f"  ✓ Audience_retention({vid}) -> {out}")
        except HttpError:
            print(f"  · Audience_retention({vid}): unsupported/insufficient data → skip")

def run_traffic_detail_per_video(yta, base_req, top_video_ids, out_dir):
    # Only try a conservative set; API will 400 silently for unsupported combos
    candidate_types = ["YT_SEARCH", "RELATED_VIDEO", "PLAYLIST", "YT_CHANNEL", "YT_OTHER_PAGE", "NO_LINK_OTHER"]
    for vid in top_video_ids:
        for t in candidate_types:
            try:
                resp = yta_query(yta, **{**base_req,
                                         "dimensions": "insightTrafficSourceDetail",
                                         "metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,engagedViews",
                                         "filters": f"video=={vid};insightTrafficSourceType=={t}",
                                         "sort": "-views"})
                out = os.path.join(out_dir, f"Traffic_source_detail__{sanitize_filename(vid)}__{t}.csv")
                headers = [h["name"] for h in resp.get("columnHeaders", [])]
                rows = resp.get("rows", [])
                save_csv(out, headers, rows)
                print(f"  ✓ Traffic_source_detail({vid}, {t}) -> {out}")
            except HttpError:
                pass  # skip unsupported

def run_playback_detail_per_video(yta, base_req, top_video_ids, out_dir):
    loc_types = ["WATCH", "EMBEDDED", "CHANNEL", "OTHER"]
    for vid in top_video_ids:
        for lt in loc_types:
            try:
                resp = yta_query(yta, **{**base_req,
                                         "dimensions": "insightPlaybackLocationDetail",
                                         "metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,engagedViews",
                                         "filters": f"video=={vid};insightPlaybackLocationType=={lt}",
                                         "sort": "-views"})
                out = os.path.join(out_dir, f"Playback_location_detail__{sanitize_filename(vid)}__{lt}.csv")
                headers = [h["name"] for h in resp.get("columnHeaders", [])]
                rows = resp.get("rows", [])
                save_csv(out, headers, rows)
                print(f"  ✓ Playback_location_detail({vid}, {lt}) -> {out}")
            except HttpError:
                pass  # skip unsupported

# =========================
# Main exporter
# =========================
def run_reports_to_csv(credentials, account_tag: str):
    yta = build("youtubeAnalytics", "v2", credentials=credentials)
    start_date, end_date = get_date_range()

    account_dir = os.path.join(REPORT_ROOT, account_tag)
    os.makedirs(account_dir, exist_ok=True)

    meta_path = os.path.join(account_dir, "_meta.txt")
    with open(meta_path, "w", encoding="utf-8") as mf:
        mf.write(f"Mode: {'CONTENT OWNER' if IS_OWNER_MODE else 'CHANNEL'}\n")
        mf.write(f"Date range: {start_date} to {end_date}\n")

    ids_val = ("contentOwner==" + CONTENT_OWNER_ID) if IS_OWNER_MODE else "channel==MINE"
    base_req = {"ids": ids_val, "startDate": start_date, "endDate": end_date}
    if IS_OWNER_MODE:
        base_req["onBehalfOfContentOwner"] = CONTENT_OWNER_ID

    # 1) Base reports (an toàn)
    for spec in build_safe_reports():
        dims = spec["dimensions"]
        mets = ",".join(spec["metrics"])
        params = dict(spec.get("params", {}))
        out_csv = os.path.join(account_dir, f"{sanitize_filename(spec['name'])}.csv")
        try:
            resp = yta_query(yta, **{**base_req, "dimensions": dims, "metrics": mets, **params})
            headers = [h.get("name", "") for h in resp.get("columnHeaders", [])]
            rows = resp.get("rows", [])
            if rows:
                save_csv(out_csv, headers, rows)
                print(f"  ✓ {sanitize_filename(spec['name'])}: {len(rows)} rows -> {out_csv}")
            else:
                if headers:
                    save_csv(out_csv, headers, [])
                print(f"  · {sanitize_filename(spec['name'])}: no_data")
        except HttpError as e:
            # Audience retention at channel-level is unsupported; we already don't query it here
            print(f"  · {sanitize_filename(spec['name'])}: unsupported → skip")

    # 2) Geography by province (US/CA) nếu có dữ liệu
    run_province_reports_if_any(yta, base_req, account_dir)

    # 3) Impressions*: chỉ nếu thật sự supported
    imp_ok = metric_supported(ids_val, start_date, end_date, "impressions", IS_OWNER_MODE)
    if imp_ok:
        try:
            resp = yta_query(yta, **{**base_req,
                                     "dimensions": "day",
                                     "metrics": "impressions,impressionsClickThroughRate,views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,engagedViews"})
            out = os.path.join(account_dir, "Impressions_by_day.csv")
            headers = [h["name"] for h in resp.get("columnHeaders", [])]
            rows = resp.get("rows", [])
            save_csv(out, headers, rows)
            print(f"  ✓ Impressions_by_day: {len(rows)} rows -> {out}")
        except HttpError:
            print("  · Impressions_by_day: unsupported → skip")

        try:
            resp = yta_query(yta, **{**base_req,
                                     "dimensions": "video",
                                     "metrics": "impressions,impressionsClickThroughRate,views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,engagedViews",
                                     "sort": "-impressions", "maxResults": 200})
            out = os.path.join(account_dir, "Impressions_by_video.csv")
            headers = [h["name"] for h in resp.get("columnHeaders", [])]
            rows = resp.get("rows", [])
            save_csv(out, headers, rows)
            print(f"  ✓ Impressions_by_video: {len(rows)} rows -> {out}")
        except HttpError:
            print("  · Impressions_by_video: unsupported → skip")
    else:
        print("  · Impressions*: not supported → skip all")

    # 4) Lấy top videos để chạy per-video detail
    top_vids = get_top_video_ids(yta, base_req, limit=50)

    # 5) Audience retention (per video)
    run_retention_per_video(yta, base_req, top_vids, account_dir)

    # 6) Traffic source detail (per video)
    run_traffic_detail_per_video(yta, base_req, top_vids, account_dir)

    # 7) Playback location detail (per video)
    run_playback_detail_per_video(yta, base_req, top_vids, account_dir)

# =========================
# Runner per credential file
# =========================
def process_one(cred_file: str):
    cred_path = os.path.join(CREDENTIALS_FOLDER, cred_file)
    account_tag = sanitize_filename(os.path.splitext(os.path.basename(cred_file))[0])
    mode = "OWNER" if IS_OWNER_MODE else "CHANNEL"
    print(f"\nProcessing {cred_file} (mode: {mode})...")

    creds = create_token_from_credentials(cred_path)
    ch = get_youtube_data(creds)
    if ch:
        print(f"  Channel: {ch['title']} ({ch['channel_id']}) | subs={ch['subs']} views={ch['views']} videos={ch['videos']}")
    run_reports_to_csv(creds, account_tag)

# =========================
# Main
# =========================
if __name__ == "__main__":
    if not os.path.isdir(CREDENTIALS_FOLDER):
        print(f"Credentials folder not found: {CREDENTIALS_FOLDER}")
        raise SystemExit(1)

    files = [f for f in os.listdir(CREDENTIALS_FOLDER) if f.lower().endswith(".json")]
    if not files:
        print("No credentials JSON found in 'credentials/'")
        raise SystemExit(0)

    for f in files:
        process_one(f)
