# module.py  — PostgreSQL only
import os
import pickle
import re
from typing import Dict, Tuple, Iterator, Optional, Set, List
from datetime import datetime, timedelta

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

from sqlalchemy import create_engine, text



# ===== Config =====
CREDENTIALS_FOLDER = "credentials"
TOKEN_FOLDER = "token"

CONTENT_OWNER_ID = os.environ.get("CONTENT_OWNER_ID", "").strip()
IS_OWNER_MODE = bool(CONTENT_OWNER_ID)

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtubepartner-channel-audit",
    "https://www.googleapis.com/auth/youtubepartner",
]

# ===== Utils =====
def sanitize_filename(s: str) -> str:
    s = s.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_\-\.]", "_", s)

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

def get_date_range(period="lifetime"):
    today = datetime.today()
    end_date = today.strftime('%Y-%m-%d')
    if period == "lifetime":
        start_date = "2005-02-14"
    else:
        raise ValueError("PG-only build: chỉ hỗ trợ period='lifetime' trong hàm này.")
    return start_date, end_date

# ===== PostgreSQL schema & upsert =====
_PG_DDL = """
CREATE TABLE IF NOT EXISTS traffic_source_daily (
  account_tag TEXT NOT NULL,
  channel_id  TEXT NOT NULL DEFAULT '',
  day         DATE NOT NULL,
  source      TEXT NOT NULL,
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

# ===== Iter helpers =====
_CHUNK_DAYS_DEFAULT = 90

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

# ===== Channel helpers =====
def get_mine_channel_id(credentials) -> Optional[str]:
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
                    part="snippet", id=channel_id,
                    onBehalfOfContentOwner=CONTENT_OWNER_ID, maxResults=1,
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

# ===== DB write =====
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

# ===== Main fetcher → Postgres =====
def run_traffic_source_lifetime_daily_to_postgres(
    credentials,
    account_tag: str,
    owner_channel_id: Optional[str] = None,
    chunk_days: int = _CHUNK_DAYS_DEFAULT,
    pg_url: Optional[str] = None,
) -> int:
    """
    Lấy Traffic Source theo NGÀY (từ ngày kênh được tạo) và lưu thẳng vào PostgreSQL.
    Trả về số dòng (day,source) đã ghi (sau khi fill).
    """
    ids, extra, owner_filters = _ids_extra_filters_for_owner(owner_channel_id)

    # lifetime window, clamped by channel creation date
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

    # Which channel_id to store (for PK)
    if IS_OWNER_MODE:
        channel_id_for_db = owner_channel_id or ""  # gộp nhiều kênh => rỗng
    else:
        channel_id_for_db = get_mine_channel_id(credentials) or ""

    # Query YouTube Analytics by chunks
    yta = build("youtubeAnalytics", "v2", credentials=credentials)
    metrics = ",".join([
        "views",
        "estimatedMinutesWatched",
        "engagedViews",
        "averageViewDuration",
        "averageViewPercentage",
    ])

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
        i_avp = col_index.get("averageViewPercentage")

        for r in rows:
            day = r[i_day]
            src = r[i_src]
            views = int(r[i_v] or 0)
            emw = int(r[i_emw] or 0)
            eng = int(r[i_eng] or 0)

            # averageViewDuration (giây) tự tính từ emw/ views
            avd = int(round((emw * 60) / views)) if views > 0 else 0
            avp = float(r[i_avp]) if (i_avp is not None and r[i_avp] is not None) else 0.0

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

    # Fill missing (day, source) with zeros so charts/aggregates are continuous
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

    out_rows = sorted(data_map.values(), key=lambda x: (x["day"], x["insightTrafficSourceType"]))

    # Save to Postgres
    save_traffic_source_daily_to_postgres(
        out_rows,
        account_tag=account_tag,
        channel_id=channel_id_for_db,
        db_url=pg_url,
    )
    print(f"[OK] Saved {len(out_rows)} rows to PostgreSQL -> traffic_source_daily (account_tag={account_tag}, channel_id='{channel_id_for_db}')")
    return len(out_rows)

# ===== One-account runner =====
def process_one(cred_file: str):
    cred_path = os.path.join(CREDENTIALS_FOLDER, cred_file)
    account_tag = sanitize_filename(os.path.splitext(os.path.basename(cred_file))[0])

    print(f"\nProcessing {cred_file} (mode: {'OWNER' if IS_OWNER_MODE else 'CHANNEL'})...")
    creds = create_token_from_credentials(cred_path)

    ch = get_youtube_data(creds)
    if ch:
        print(f"  Channel: {ch['title']} ({ch['channel_id']}) | subs={ch['subs']} views={ch['views']} videos={ch['videos']}")

    # Save lifetime daily traffic_source directly to Postgres
    run_traffic_source_lifetime_daily_to_postgres(
        credentials=creds,
        account_tag=account_tag,
        owner_channel_id=None,  # set UCxxx nếu muốn lọc 1 kênh trong OWNER mode
        pg_url=os.getenv("PG_URL"),  # hoặc truyền thẳng chuỗi URL
    )