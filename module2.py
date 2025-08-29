import os
import csv
import pickle
import re
from typing import Dict, Any, List
from datetime import datetime, timedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from typing import Dict, Tuple, Iterator, Optional, Set, List

from sqlalchemy import create_engine, text

CREDENTIALS_FOLDER = "credentials"
TOKEN_FOLDER = "token"
REPORT_ROOT = "reports"

CONTENT_OWNER_ID = os.environ.get("CONTENT_OWNER_ID", "").strip()
IS_OWNER_MODE = bool(CONTENT_OWNER_ID)

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtubepartner-channel-audit",
    "https://www.googleapis.com/auth/youtubepartner",
]

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


def get_date_range(period="30d"):
    today = datetime.today()
    end_date = today.strftime('%Y-%m-%d')

    if period == "7d":
        start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    elif period == "28d":
        start_date = (today - timedelta(days=28)).strftime('%Y-%m-%d')
    elif period == "90d":
        start_date = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    elif period == "365d":
        start_date = (today - timedelta(days=365)).strftime('%Y-%m-%d')
    elif period == "30d":
        start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    elif period == "lifetime":
        start_date = "2005-02-14"
    elif period == "2025":
        start_date, end_date = "2025-01-01", "2025-12-31"
    elif period == "2024":
        start_date, end_date = "2024-01-01", "2024-12-31"
    else:
        raise ValueError("Chỉ hỗ trợ: '7d','28d','90d','365d','30d','lifetime','2025','2024'")

    return start_date, end_date


PERIODS = ["7d","28d","90d","365d","30d","lifetime","2025","2024"]

def run_traffic_reports_to_csv(credentials, account_tag: str, periods: List[str] = PERIODS):
    yta = build("youtubeAnalytics", "v2", credentials=credentials)

    # Thư mục đích cho traffic source
    out_dir = os.path.join(REPORT_ROOT, account_tag, "traffic_source")
    os.makedirs(out_dir, exist_ok=True)

    # Chỉ 1 report spec
    base_mets = ["views", "estimatedMinutesWatched", "averageViewDuration", "averageViewPercentage", "engagedViews"]
    report_spec = {
        "file_stub": "TrafficSource",
        "dimensions": "insightTrafficSourceType",
        "metrics": base_mets,
        "params": {}
    }

    for period in periods:
        start_date, end_date = get_date_range(period)

        dims = report_spec["dimensions"]
        mets = ",".join(report_spec["metrics"])
        params = dict(report_spec.get("params", {}))

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

        out_csv = os.path.join(out_dir, f"{report_spec['file_stub']}.{period}.csv")

        try:
            resp = yta.reports().query(**req).execute()
            headers = [h.get("name", "") for h in resp.get("columnHeaders", [])]
            rows = resp.get("rows", []) or []
            save_csv(out_csv, headers, rows)
            print(f"  ✓ traffic_sources ({period}): {len(rows)} rows -> {out_csv}")
        except HttpError as e:
            print(f"  ✗ traffic_sources ({period}): error ({getattr(e, 'status_code', 'HttpError')})")
            save_csv(out_csv, report_spec.get("metrics", []), [])
        except Exception as e:
            print(f"  ✗ traffic_sources ({period}): error ({e.__class__.__name__})")
            save_csv(out_csv, [], [])

def run_geography_reports_to_csv(
    credentials,
    account_tag: str,
    periods,
):

    yta = build("youtubeAnalytics", "v2", credentials=credentials)

    base_dir = os.path.join(REPORT_ROOT, account_tag, "geography")
    os.makedirs(base_dir, exist_ok=True)

    # Các metric cần lấy để tính đủ cột yêu cầu
    wanted_metrics = [
        "views",
        "engagedViews",
        "estimatedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
        "subscribersGained",
        "subscribersLost",
    ]

    def _ids_dict():
        ids = ("contentOwner==" + CONTENT_OWNER_ID) if IS_OWNER_MODE else "channel==MINE"
        extra = {}
        if IS_OWNER_MODE:
            extra["onBehalfOfContentOwner"] = CONTENT_OWNER_ID
        return ids, extra

    for period in periods:
        start_date, end_date = get_date_range(period)
        suffix = sanitize_filename(str(period))  # vd: 7d, 28d, 2025, Last_28_days,...

        try:
            ids, extra = _ids_dict()
            req = {
                "ids": ids,
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": "country",
                "metrics": ",".join(wanted_metrics),
                "sort": "-views",
            }
            req.update(extra)

            resp = yta.reports().query(**req).execute()
            headers = [h.get("name", "") for h in resp.get("columnHeaders", [])]
            rows = resp.get("rows", []) or []

            # map header -> index
            idx = {name: i for i, name in enumerate(headers)}

            # Header CSV theo yêu cầu
            out_headers = [
                "country",
                "views",
                "engagedViews",
                "watchTimeHours",
                "subscribers",
                "averageViewDuration",
                "averageViewPercentage",
            ]

            # Chuyển đổi hàng dữ liệu
            out_rows = []
            for r in rows:
                country = r[idx["country"]]
                views = r[idx.get("views", -1)] if "views" in idx else 0
                engaged = r[idx.get("engagedViews", -1)] if "engagedViews" in idx else 0
                minutes = r[idx.get("estimatedMinutesWatched", -1)] if "estimatedMinutesWatched" in idx else 0
                avg_dur = r[idx.get("averageViewDuration", -1)] if "averageViewDuration" in idx else 0
                avg_pct = r[idx.get("averageViewPercentage", -1)] if "averageViewPercentage" in idx else 0
                sub_g = r[idx.get("subscribersGained", -1)] if "subscribersGained" in idx else 0
                sub_l = r[idx.get("subscribersLost", -1)] if "subscribersLost" in idx else 0

                watch_hours = round((float(minutes) if minutes is not None else 0.0) / 60.0, 2)
                subs_net = int((float(sub_g) if sub_g is not None else 0.0) - (float(sub_l) if sub_l is not None else 0.0))

                out_rows.append([
                    country,
                    views,
                    engaged,
                    watch_hours,
                    subs_net,
                    avg_dur,
                    avg_pct,
                ])

            out_csv = os.path.join(base_dir, f"Geographic.{suffix}.csv")
            save_csv(out_csv, out_headers, out_rows)
            print(f"  ✓ Geography (country) {period}: {len(out_rows)} rows -> {out_csv}")

        except HttpError as e:
            print(f"  ✗ Geography (country) {period}: error ({e.status_code})")
        except Exception as e:
            print(f"  ✗ Geography (country) {period}: error ({e.__class__.__name__})")


def build_reports():
    base_mets = ["views", "estimatedMinutesWatched"]
    avg_mets = ["averageViewDuration", "averageViewPercentage"]

    reports = [
        # Time series
        {
            "name": "Daily summary",
            "dimensions": "day",
            "metrics": base_mets + avg_mets + [
                "subscribersGained", "subscribersLost",
                "likes", "shares", "comments"
            ],
            "params": {"sort": "day"}
        },
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

def run_reports_to_csv(credentials, account_tag: str, date_rage):
    yta = build("youtubeAnalytics", "v2", credentials=credentials)
    start_date, end_date = get_date_range(date_rage)
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

def process_one(cred_file: str):
        cred_path = os.path.join(CREDENTIALS_FOLDER, cred_file)
        account_tag = sanitize_filename(os.path.splitext(os.path.basename(cred_file))[0])

        print(f"\nProcessing {cred_file} (mode: {'OWNER' if IS_OWNER_MODE else 'CHANNEL'})...")
        creds = create_token_from_credentials(cred_path)

        ch = get_youtube_data(creds)
        if ch:
            print(f"  Channel: {ch['title']} ({ch['channel_id']}) | subs={ch['subs']} views={ch['views']} videos={ch['videos']}")

        #nhiều periods
        # run_traffic_reports_to_csv(creds, account_tag, PERIODS)
        # run_geography_reports_to_csv(creds,account_tag, PERIODS)

        # (tuỳ chọn) nếu vẫn muốn xuất toàn bộ báo cáo khác:
        # run_reports_to_csv(creds, account_tag, date_rage="30d")

        run_traffic_source_lifetime_daily_to_csv(
            credentials=creds,
            account_tag=account_tag,
            owner_channel_id=None,
            pg_url="postgresql+psycopg2://postgres:V!etdu1492003@localhost:5432/analytics"
        )







########################################################################################################
import csv, io, os, sys, hashlib, re, glob


INPUT_ROOT = r"C:\Users\Admin\Documents\dev\26_8_2025\python_backend\reports"

OUTPUT_ROOT = r"C:\Users\Admin\Documents\dev\26_8_2025\react-dashboard\src\data\channels"

BASE_OUTNAME = "TrafficSource"

# Danh sách period hợp lệ
PERIODS = {"7d", "28d", "90d", "365d", "30d", "lifetime", "2025", "2024"}

# ============ HELPERS ============

def sniff_delimiter(sample_bytes: bytes, fallback=","):
    try:
        dialect = csv.Sniffer().sniff(sample_bytes.decode("utf-8", errors="ignore"))
        return dialect.delimiter
    except Exception:
        return fallback

def hsl_from_text(s: str) -> str:
    h = int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16) % 360
    return f"hsl({h}, 70%, 50%)"

def to_number(s, default=0.0):
    try:
        return float((s or "0").replace(",", ""))
    except Exception:
        return default

def safe_js_str(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')

def parse_csv_items(csv_path: str):
    with open(csv_path, "rb") as fb:
        raw = fb.read()
    if not raw.strip():
        return []

    delim = sniff_delimiter(raw[:4096])
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    if not reader.fieldnames:
        return []

    rows = [
        {(k or "").strip(): (v or "").strip() for k, v in r.items()}
        for r in reader
        if any((v or "").strip() for v in r.values())
    ]

    items = []
    for r in rows:
        idv = r.get("insightTrafficSourceType") or r.get("id") or ""
        if not idv:
            continue
        item = {
            "id": idv,
            "label": idv,
            "value": to_number(r.get("views", "0")),
            "color": hsl_from_text(idv),
            "views": int(to_number(r.get("views", "0"))),
            "estimatedMinutesWatched": int(to_number(r.get("estimatedMinutesWatched", "0"))),
            "averageViewDuration": int(to_number(r.get("averageViewDuration", "0"))),
            "averageViewPercentage": float(to_number(r.get("averageViewPercentage", "0"))),
            "engagedViews": int(to_number(r.get("engagedViews", "0"))),
        }
        items.append(item)

    items.sort(key=lambda x: x["views"], reverse=True)
    return items

_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_\-\.]+")
def sanitize_path_component(name: str) -> str:
    name = name.strip().replace(" ", "_")
    return _SANITIZE_RE.sub("_", name) or "_"

def extract_period_or_basename(csv_path: str):
    """
    Trả về (tag, is_period):
      - Nếu tên file match traffic_sources__<period>.csv (hoặc biến thể),
        trả về (period, True)
      - Ngược lại: (basename_khong_ext_sanitized, False)
    """
    name = os.path.splitext(os.path.basename(csv_path))[0]
    rx = re.compile(
        r"^traffic[_\-\.\s]*sources?[_\-\.\s]+(7d|28d|90d|365d|30d|lifetime|2025|2024)$",
        re.IGNORECASE,
    )
    m = rx.match(name)
    if m:
        return (m.group(1).lower(), True)
    # fallback: dùng chính basename
    return (sanitize_path_component(name), False)

def write_js(out_path: str, items, tag: str, is_period: bool):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("// Auto-generated. Do not edit manually.\n")
        if is_period:
            out.write(f'export const period = "{tag}";\n')
        else:
            out.write(f'export const datasetTag = "{tag}";\n')
        out.write("export const traffic_source = [\n")
        for it in items:
            out.write("  {\n")
            for k, v in it.items():
                if isinstance(v, (int, float)):
                    out.write(f"    {k}: {v},\n")
                else:
                    out.write(f'    {k}: "{safe_js_str(v)}",\n')
            out.write("  },\n")
        out.write("];\n")
        out.write("export default traffic_source;\n")

# ============ MAIN ============

def convert_to_js():
    if not os.path.isdir(INPUT_ROOT):
        print(f"Không tìm thấy thư mục: {INPUT_ROOT}")
        sys.exit(1)

    made = 0
    skipped = 0

    # Duyệt đệ quy qua tất cả thư mục con
    for dirpath, dirnames, filenames in os.walk(INPUT_ROOT):
        # Lấy danh sách *.csv trong thư mục hiện tại
        csv_files = [os.path.join(dirpath, f) for f in filenames if f.lower().endswith(".csv")]
        if not csv_files:
            continue

        # Tính đường dẫn tương đối so với INPUT_ROOT để giữ cấu trúc
        rel_dir = os.path.relpath(dirpath, INPUT_ROOT)
        # Sanitize từng thành phần thư mục để tránh ký tự lạ
        safe_parts = [sanitize_path_component(p) for p in rel_dir.split(os.sep) if p not in (".", "")]
        safe_rel_dir = os.path.join(*safe_parts) if safe_parts else ""
        out_base_dir = os.path.join(OUTPUT_ROOT, safe_rel_dir)

        # Xử lý từng CSV
        for csv_path in sorted(csv_files):
            try:
                items = parse_csv_items(csv_path)
            except Exception as e:
                print(f"✗ Lỗi đọc {csv_path}: {e}")
                skipped += 1
                continue

            tag, is_period = extract_period_or_basename(csv_path)  
            base_no_ext = os.path.splitext(os.path.basename(csv_path))[0]
            out_name = base_no_ext + ".js"  
            out_path = os.path.join(out_base_dir, out_name)


            write_js(out_path, items, tag, is_period)
            made += 1
            print(f"→ Xuất {out_path} ({len(items)} items)")

    if made == 0:
        print("Không tạo được file JS nào.")
        sys.exit(1)

    print(f"Hoàn tất: đã tạo {made} file JS trong {OUTPUT_ROOT}. Skipped: {skipped}.")


##########################################line/bar chart#####################################

_PG_DDL = """
CREATE TABLE IF NOT EXISTS traffic_source_daily (
  account_tag TEXT NOT NULL,
  channel_id  TEXT NOT NULL DEFAULT '',
  day         DATE NOT NULL,
  source      TEXT NOT NULL,               -- insightTrafficSourceType
  views       INTEGER NOT NULL,
  estimated_minutes_watched INTEGER NOT NULL,
  average_view_duration     INTEGER NOT NULL,
  average_view_percentage   DOUBLE PRECISION NOT NULL,
  engaged_views             INTEGER NOT NULL,
  PRIMARY KEY (account_tag, channel_id, day, source)
);
CREATE INDEX IF NOT EXISTS idx_tsd_day    ON traffic_source_daily(day);
CREATE INDEX IF NOT EXISTS idx_tsd_source ON traffic_source_daily(source);
CREATE INDEX IF NOT EXISTS idx_tsd_acct   ON traffic_source_daily(account_tag);
"""

_PG_UPSERT = """
INSERT INTO traffic_source_daily
 (account_tag, channel_id, day, source,
  views, estimated_minutes_watched, average_view_duration,
  average_view_percentage, engaged_views)
VALUES (:account_tag, :channel_id, :day, :source,
        :views, :emw, :avd, :avp, :eng)
ON CONFLICT (account_tag, channel_id, day, source) DO UPDATE SET
  views                     = EXCLUDED.views,
  estimated_minutes_watched = EXCLUDED.estimated_minutes_watched,
  average_view_duration     = EXCLUDED.average_view_duration,
  average_view_percentage   = EXCLUDED.average_view_percentage,
  engaged_views             = EXCLUDED.engaged_views
"""

def _chunks(seq: List[Dict], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def save_traffic_source_daily_to_postgres(
    out_rows: List[Dict],
    account_tag: str,
    channel_id: Optional[str] = None,
    db_url: Optional[str] = None,
    batch_size: int = 5000,
):
    db_url = db_url or os.getenv("PG_URL")
    if not db_url:
        raise ValueError("Thiếu db_url. Truyền db_url hoặc đặt biến môi trường PG_URL.")

    ch_id = channel_id or ""  # NOT NULL DEFAULT '' theo PK

    engine = create_engine(db_url, pool_pre_ping=True, future=True)

    with engine.begin() as conn:
        for stmt in _PG_DDL.strip().split(";\n"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

    payload = [{
        "account_tag": account_tag,
        "channel_id": ch_id,
        "day": r["day"],  # YYYY-MM-DD
        "source": r["insightTrafficSourceType"],
        "views": int(r.get("views", 0) or 0),
        "emw": int(r.get("estimatedMinutesWatched", 0) or 0),
        "avd": int(r.get("averageViewDuration", 0) or 0),
        "avp": float(r.get("averageViewPercentage", 0.0) or 0.0),
        "eng": int(r.get("engagedViews", 0) or 0),
    } for r in out_rows]

    with engine.begin() as conn:
        for chunk in _chunks(payload, batch_size):
            conn.execute(text(_PG_UPSERT), chunk)

# === Helpers sẵn có của bạn ===
_CHUNK_DAYS_DEFAULT = 60

def _iter_days(start: str, end: str) -> Iterator[str]:
    sd = datetime.strptime(start, "%Y-%m-%d").date()
    ed = datetime.strptime(end, "%Y-%m-%d").date()
    cur = sd
    while cur <= ed:
        yield cur.isoformat()
        cur += timedelta(days=1)

def _iter_day_chunks(start: str, end: str, chunk_days: int = _CHUNK_DAYS_DEFAULT) -> Iterator[Tuple[str, str]]:
    sd = datetime.strptime(start, "%Y-%m-%d").date()
    ed = datetime.strptime(end, "%Y-%m-%d").date()
    cur = sd
    while cur <= ed:
        nxt = min(cur + timedelta(days=chunk_days - 1), ed)
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + timedelta(days=1)

def _safe_date_ymd(d: str) -> str:
    return d[:10]

def get_mine_channel_id(credentials) -> Optional[str]:
    """Lấy channelId khi chạy channel mode (mine=True)."""
    try:
        yt = build("youtube", "v3", credentials=credentials)
        resp = yt.channels().list(part="id", mine=True, maxResults=1).execute() or {}
        items = resp.get("items", [])
        if items:
            return items[0]["id"]
    except Exception as e:
        print(f"[WARN] get_mine_channel_id failed: {e}")
    return None

def get_channel_created_date(credentials, channel_id: Optional[str] = None) -> Optional[str]:
    yt = build("youtube", "v3", credentials=credentials)
    try:
        if IS_OWNER_MODE and CONTENT_OWNER_ID:
            if channel_id:
                resp = yt.channels().list(
                    part="snippet",
                    id=channel_id,
                    onBehalfOfContentOwner=CONTENT_OWNER_ID,
                    maxResults=1,
                ).execute() or {}
                items = resp.get("items", [])
                if items:
                    return _safe_date_ymd(items[0]["snippet"]["publishedAt"])

            req = yt.channels().list(
                part="snippet",
                managedByMe=True,
                onBehalfOfContentOwner=CONTENT_OWNER_ID,
                maxResults=50,
            )
            dates = []
            while req is not None:
                resp = req.execute() or {}
                for it in resp.get("items", []):
                    dates.append(_safe_date_ymd(it["snippet"]["publishedAt"]))
                req = yt.channels().list_next(req, resp)
            if dates:
                return min(dates)
        else:
            resp = yt.channels().list(part="snippet", mine=True, maxResults=1).execute() or {}
            items = resp.get("items", [])
            if items:
                return _safe_date_ymd(items[0]["snippet"]["publishedAt"])
    except Exception as e:
        print(f"[WARN] get_channel_created_date failed: {e}")
    return None

def _ids_extra_filters_for_owner(owner_channel_id: Optional[str]) -> Tuple[str, Dict, Optional[str]]:
    if IS_OWNER_MODE and CONTENT_OWNER_ID:
        ids = f"contentOwner=={CONTENT_OWNER_ID}"
        extra = {"onBehalfOfContentOwner": CONTENT_OWNER_ID}
        if owner_channel_id:
            return ids, extra, f"channel=={owner_channel_id};claimedStatus==claimed;uploaderType==self"
        return ids, extra, "claimedStatus==claimed;uploaderType==self"
    return "channel==MINE", {}, None

# ====== HÀM CHÍNH (đã thêm lưu Postgres & lấy channel_id) ======
def run_traffic_source_lifetime_daily_to_csv(
    credentials,
    account_tag: str,
    owner_channel_id: Optional[str] = None,
    chunk_days: int = _CHUNK_DAYS_DEFAULT,
    out_filename: str = "TrafficSourceDaily_lifetime.csv",
    pg_url: Optional[str] = None,   # <--- thêm param để lưu Postgres
):
    """
    Lấy Traffic Source theo NGÀY từ ngày kênh được tạo; fill missing; ghi CSV; lưu Postgres.
    """
    ids, extra, owner_filters = _ids_extra_filters_for_owner(owner_channel_id)

    # Lifetime base & clamp về ngày tạo kênh
    start_date, end_date = get_date_range("lifetime")
    created = get_channel_created_date(credentials, channel_id=owner_channel_id)
    if created:
        try:
            s0 = datetime.strptime(start_date, "%Y-%m-%d").date()
            sc = datetime.strptime(created, "%Y-%m-%d").date()
            if sc > s0:
                start_date = sc.isoformat()
        except Exception:
            pass

    # Channel id để lưu DB:
    if IS_OWNER_MODE:
        # Nếu owner chỉ định 1 kênh -> dùng UC đó; nếu gộp nhiều kênh -> rỗng ''
        channel_id_for_db = owner_channel_id or ""
    else:
        channel_id_for_db = get_mine_channel_id(credentials) or ""

    # Output CSV path
    base_dir = os.path.join(REPORT_ROOT, account_tag, "traffic_source_daily")
    os.makedirs(base_dir, exist_ok=True)
    out_csv = os.path.join(base_dir, sanitize_filename(out_filename))

    # Query Analytics theo block
    yta = build("youtubeAnalytics", "v2", credentials=credentials)
    metrics = "views,estimatedMinutesWatched,engagedViews"

    data_map: Dict[Tuple[str, str], Dict] = {}
    source_set: Set[str] = set()

    for sd, ed in _iter_day_chunks(start_date, end_date, chunk_days=chunk_days):
        q = {
            "ids": ids,
            "startDate": sd,
            "endDate": ed,
            "dimensions": "insightTrafficSourceType,day",
            "metrics": metrics,
            "sort": "day,insightTrafficSourceType",
            **extra,
        }
        if owner_filters:
            q["filters"] = owner_filters

        try:
            resp = yta.reports().query(**q).execute() or {}
        except HttpError as he:
            print(f"[WARN] Analytics query failed {sd}..{ed}: {he}")
            continue

        rows = resp.get("rows") or []
        if not rows:
            continue

        col_index = {c["name"]: i for i, c in enumerate(resp.get("columnHeaders", []))}
        i_day = col_index.get("day")
        i_src = col_index.get("insightTrafficSourceType")
        i_v   = col_index.get("views")
        i_emw = col_index.get("estimatedMinutesWatched")
        i_eng = col_index.get("engagedViews")

        for r in rows:
            day = r[i_day]
            src = r[i_src]
            views = int(r[i_v] or 0)
            emw = int(r[i_emw] or 0)
            eng = int(r[i_eng] or 0)
            avd = int(round((emw * 60) / views)) if views > 0 else 0
            avp = 0.0

            data_map[(day, src)] = {
                "day": day,
                "insightTrafficSourceType": src,
                "views": views,
                "estimatedMinutesWatched": emw,
                "averageViewDuration": avd,
                "averageViewPercentage": avp,
                "engagedViews": eng,
            }
            source_set.add(src)

    # Fill missing days
    all_days = list(_iter_days(start_date, end_date))
    for d in all_days:
        for s in source_set:
            if (d, s) not in data_map:
                data_map[(d, s)] = {
                    "day": d,
                    "insightTrafficSourceType": s,
                    "views": 0,
                    "estimatedMinutesWatched": 0,
                    "averageViewDuration": 0,
                    "averageViewPercentage": 0.0,
                    "engagedViews": 0,
                }

    # Xuất CSV
    headers = [
        "day",
        "insightTrafficSourceType",
        "views",
        "estimatedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
        "engagedViews",
    ]
    out_rows = sorted(data_map.values(), key=lambda x: (x["day"], x["insightTrafficSourceType"]))
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(out_rows)

    print(f"[OK] TrafficSourceDaily (from channel created, filled) -> {out_csv}")

    # Lưu Postgres (nếu cung cấp pg_url hoặc đã set PG_URL)
    if pg_url or os.getenv("PG_URL"):
        save_traffic_source_daily_to_postgres(
            out_rows,
            account_tag=account_tag,
            channel_id=channel_id_for_db,
            db_url=pg_url,
        )
        print("[OK] Saved to PostgreSQL -> table traffic_source_daily")
    else:
        print("[SKIP] PostgreSQL: thiếu PG_URL hoặc pg_url")