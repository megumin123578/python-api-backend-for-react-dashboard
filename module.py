import os
import csv
import pickle
import re
from typing import Dict, Any, List

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

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


from datetime import datetime,timedelta

from datetime import datetime, timedelta

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
        run_traffic_reports_to_csv(creds, account_tag, PERIODS)
        run_geography_reports_to_csv(creds,account_tag, PERIODS)

        # (tuỳ chọn) nếu vẫn muốn xuất toàn bộ báo cáo khác:
        run_reports_to_csv(creds, account_tag, date_rage="30d")



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





