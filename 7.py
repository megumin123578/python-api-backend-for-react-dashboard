# yt_analytics_csv.py
import os
import csv
import pickle
import re
from typing import Dict, Any, List

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from module import get_date_range 

# =========================
# Paths / Settings
# =========================
CREDENTIALS_FOLDER = "credentials"
TOKEN_FOLDER = "token"
REPORT_ROOT = "reports"

# Set CONTENT_OWNER_ID (External ID) for Owner mode; leave empty for Channel mode
CONTENT_OWNER_ID = os.environ.get("CONTENT_OWNER_ID", "").strip()
IS_OWNER_MODE = bool(CONTENT_OWNER_ID)

# Scopes (đủ cho Analytics + đọc YouTube; có thêm partner nếu ở owner mode)
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

# =========================
# Auth helpers
# =========================
def create_token_from_credentials(cred_path: str):
    """
    Create/refresh OAuth token from a client_secrets JSON.
    Token saved as token/<name>.pickle
    """
    os.makedirs(TOKEN_FOLDER, exist_ok=True)
    token_filename = os.path.splitext(os.path.basename(cred_path))[0] + ".pickle"
    token_path = os.path.join(TOKEN_FOLDER, token_filename)

    creds = None
    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)
    return creds

# =========================
# Optional: basic channel snapshot (log)
# =========================
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
# Reports registry
# =========================
def build_reports():
    base_mets = ["views", "estimatedMinutesWatched"]
    avg_mets = ["averageViewDuration", "averageViewPercentage"]

    reports = [
        # Time series
        {"name": "Time by day", "dimensions": "day", "metrics": base_mets + avg_mets},
        {"name": "Subs by day", "dimensions": "day", "metrics": ["subscribersGained", "subscribersLost", "views"]},
        {"name": "Engagement by day", "dimensions": "day", "metrics": ["likes", "shares", "comments", "views"]},

        # Geography
        {"name": "Geography by country", "dimensions": "country", "metrics": base_mets + avg_mets},
        {"name": "Geography by province", "dimensions": "province", "metrics": base_mets},

        # Devices / OS
        {"name": "Devices (deviceType)", "dimensions": "deviceType", "metrics": base_mets},
        {"name": "Operating system", "dimensions": "operatingSystem", "metrics": base_mets},

        # Traffic / Playback (đã mở rộng theo yêu cầu)
        {"name": "Traffic sources",
         "dimensions": "insightTrafficSourceType",
         "metrics": base_mets + avg_mets + ["engagedViews"]},

        {"name": "Traffic source detail",
         "dimensions": "insightTrafficSourceDetail",
         "metrics": base_mets + avg_mets + ["engagedViews"],
         "params": {"sort": "-views"}},

        {"name": "Playback locations", "dimensions": "insightPlaybackLocationType",
         "metrics": base_mets + avg_mets + ["engagedViews"]},
        {"name": "Playback location detail", "dimensions": "insightPlaybackLocationDetail",
         "metrics": base_mets + avg_mets + ["engagedViews"],
         "params": {"sort": "-views"}},

        # Audience & misc
        {"name": "Subscribed status", "dimensions": "subscribedStatus", "metrics": base_mets},
        {"name": "Live or OnDemand", "dimensions": "liveOrOnDemand", "metrics": base_mets},
        {"name": "Uploader type", "dimensions": "uploaderType", "metrics": base_mets},
        {"name": "Sharing service", "dimensions": "sharingService", "metrics": base_mets},
        {"name": "Subtitle language", "dimensions": "subtitleLanguage", "metrics": base_mets},

        # Demographics
        {"name": "Demographics (ageGroup,gender)", "dimensions": "ageGroup,gender",
         "metrics": ["viewerPercentage"], "params": {"sort": "-viewerPercentage"}},

        # Audience retention (có thể rỗng)
        {"name": "Audience retention (elapsed)", "dimensions": "elapsedVideoTimeRatio",
         "metrics": ["audienceWatchRatio"], "params": {"filters": "audienceType==ORGANIC"}},

        # Top videos / playlists
        {"name": "Top videos", "dimensions": "video",
         "metrics": ["views", "estimatedMinutesWatched", "averageViewDuration", "engagedViews"],
         "params": {"sort": "-views", "maxResults": 200}},
        {"name": "Top playlists", "dimensions": "playlist",
         "metrics": base_mets, "params": {"sort": "-views", "maxResults": 200}},

        # Cards & Annotations (giữ nguyên)
        {"name": "Cards by day", "dimensions": "day",
         "metrics": ["cardImpressions", "cardClicks", "cardClickRate",
                     "cardTeaserImpressions", "cardTeaserClicks", "cardTeaserClickRate"]},
        {"name": "Annotations by day", "dimensions": "day",
         "metrics": ["annotationImpressions", "annotationClickableImpressions",
                     "annotationClicks", "annotationClickThroughRate",
                     "annotationClosableImpressions", "annotationCloses", "annotationCloseRate"]},

        # --- NEW: Impressions reports (để lấy impressions + CTR) ---
        {"name": "Impressions by day",
         "dimensions": "day",
         "metrics": ["impressions", "impressionsClickThroughRate",
                     "views", "estimatedMinutesWatched",
                     "averageViewDuration", "averageViewPercentage", "engagedViews"]},

        {"name": "Impressions by video",
         "dimensions": "video",
         "metrics": ["impressions", "impressionsClickThroughRate",
                     "views", "estimatedMinutesWatched",
                     "averageViewDuration", "averageViewPercentage", "engagedViews"],
         "params": {"sort": "-impressions", "maxResults": 200}},
    ]

    if IS_OWNER_MODE:
        owner_reports = [
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
        reports.extend(owner_reports)

    return reports


# =========================
# Fetcher (save CSVs vào thư mục con theo credentials)
# =========================
def run_reports_to_csv(credentials, account_tag: str):
    yta = build("youtubeAnalytics", "v2", credentials=credentials)
    start_date, end_date = get_date_range()
    reports = build_reports()

    # Tạo thư mục con: reports/<account_tag>/
    account_dir = os.path.join(REPORT_ROOT, account_tag)
    os.makedirs(account_dir, exist_ok=True)

    # metadata (tùy chọn)
    meta_path = os.path.join(account_dir, "_meta.txt")
    with open(meta_path, "w", encoding="utf-8") as mf:
        mf.write(f"Mode: {'CONTENT OWNER' if IS_OWNER_MODE else 'CHANNEL'}\n")
        mf.write(f"Date Range: {start_date} to {end_date}\n")

    for spec in reports:
        dims = spec["dimensions"]
        mets = ",".join(spec["metrics"])
        params = spec.get("params", {})

        req = {
            "ids": ("contentOwner==" + CONTENT_OWNER_ID) if IS_OWNER_MODE else "channel==MINE",
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dims,
            "metrics": mets,
        }
        req.update(params)
        if IS_OWNER_MODE:
            req["onBehalfOfContentOwner"] = CONTENT_OWNER_ID

        # Tên file CSV trong thư mục con
        report_name = sanitize_filename(spec["name"])
        out_csv = os.path.join(account_dir, f"{report_name}.csv")

        try:
            resp = yta.reports().query(**req).execute()
            headers = [h.get("name", "") for h in resp.get("columnHeaders", [])]
            rows = resp.get("rows", [])

            if rows:
                save_csv(out_csv, headers, rows)
                print(f"  ✓ {spec['name']}: {len(rows)} rows -> {out_csv}")
            else:
                # vẫn tạo CSV rỗng với header (nếu có)
                if headers:
                    save_csv(out_csv, headers, [])
                print(f"  · {spec['name']}: no_data")
        except HttpError as e:
            # Audience retention có thể không có dữ liệu với một số kênh/date range -> tạo CSV rỗng thay vì fail
            if "audienceWatchRatio" in mets and "elapsedVideoTimeRatio" in dims:
                if headers := ["elapsedVideoTimeRatio", "audienceWatchRatio"]:
                    save_csv(out_csv, headers, [])
                print(f"  · {spec['name']}: no_data (audience retention)")
            else:
                print(f"  ✗ {spec['name']}: error ({e.status_code})")
        except Exception as e:
            print(f"  ✗ {spec['name']}: error ({e.__class__.__name__})")

# =========================
# Main
# =========================
def main():
    if not os.path.exists(CREDENTIALS_FOLDER):
        print(f"Credentials folder '{CREDENTIALS_FOLDER}' does not exist.")
        return
    os.makedirs(TOKEN_FOLDER, exist_ok=True)
    os.makedirs(REPORT_ROOT, exist_ok=True)

    files = [f for f in os.listdir(CREDENTIALS_FOLDER) if f.endswith(".json")]
    if not files:
        print(f"No credentials files found in {CREDENTIALS_FOLDER}.")
        return

    token_files = [f for f in os.listdir(TOKEN_FOLDER) if f.endswith(".pickle")]

    def process_one(cred_file: str):
        cred_path = os.path.join(CREDENTIALS_FOLDER, cred_file)
        account_tag = sanitize_filename(os.path.splitext(os.path.basename(cred_file))[0])

        print(f"\nProcessing {cred_file} (mode: {'OWNER' if IS_OWNER_MODE else 'CHANNEL'})...")
        creds = create_token_from_credentials(cred_path)

        ch = get_youtube_data(creds)
        if ch:
            print(f"  Channel: {ch['title']} ({ch['channel_id']}) | subs={ch['subs']} views={ch['views']} videos={ch['videos']}")

        run_reports_to_csv(creds, account_tag)

    if len(files) == len(token_files):
        print("All credentials have tokens. Processing all accounts...")
        for cred_file in files:
            process_one(cred_file)
    else:
        print("Available credentials files:")
        for i, file in enumerate(files):
            print(f"{i + 1}. {file}")
        try:
            choice = int(input("Select a file by number: ")) - 1
            if choice < 0 or choice >= len(files):
                print("Invalid choice.")
                return
            process_one(files[choice])
        except ValueError:
            print("Invalid input. Please enter a number.")

if __name__ == "__main__":
    main()
