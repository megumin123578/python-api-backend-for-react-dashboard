from fastapi import APIRouter, Query
from datetime import datetime, timedelta
from module_trafficsource import create_token_from_credentials
from module_geography import fetch_geography

import os

router = APIRouter(prefix="/api/geography")

CREDENTIALS_DIR = "./credentials"

def load_all_credentials():
    creds = {}
    for fname in os.listdir(CREDENTIALS_DIR):
        if fname.endswith(".json"):
            channel = fname.replace(".json", "")
            full_path = os.path.join(CREDENTIALS_DIR, fname)
            creds[channel] = full_path
    return creds


def get_range_dates(range_key: str):
    today = datetime.today().date()

    if range_key == "7d":   return today - timedelta(days=6), today
    if range_key == "28d":  return today - timedelta(days=27), today
    if range_key == "90d":  return today - timedelta(days=89), today
    if range_key == "365d": return today - timedelta(days=364), today
    if range_key == "lifetime":
        return datetime(2005, 2, 14).date(), today

    if range_key.isdigit():
        year = int(range_key)
        return datetime(year, 1, 1).date(), datetime(year, 12, 31).date()

    return None


@router.get("/")
def api_geography(
    range: str = None,
    month: str = None,
    start: str = None,
    end: str = None,
    channel: str = None,
):
    CHANNEL_CREDENTIALS = load_all_credentials()

    # Nếu channel = None → không chọn gì → trả về availableChannels
    if not channel:
        return {
            "start": None,
            "end": None,
            "channel": None,
            "rows": [],
            "availableChannels": list(CHANNEL_CREDENTIALS.keys()),
        }

    if channel not in CHANNEL_CREDENTIALS:
        return {
            "start": None,
            "end": None,
            "channel": channel,
            "rows": [],
            "availableChannels": list(CHANNEL_CREDENTIALS.keys()),
        }

    cred_file = CHANNEL_CREDENTIALS[channel]

    try:
        creds = create_token_from_credentials(cred_file)
    except Exception as e:
        return {
            "start": None,
            "end": None,
            "channel": channel,
            "rows": [],
            "error": "invalid_credentials",
            "availableChannels": list(CHANNEL_CREDENTIALS.keys()),
        }

    # ========= date range logic =========
    if range:
        s, e = get_range_dates(range)
    elif month:
        y, m = map(int, month.split("-"))
        s = datetime(y, m, 1).date()
        if m == 12:
            e = datetime(y, 12, 31).date()
        else:
            e = (datetime(y, m + 1, 1) - timedelta(days=1)).date()
    else:
        s = datetime.strptime(start, "%Y-%m-%d").date()
        e = datetime.strptime(end, "%Y-%m-%d").date()

    # ========= Fetch data from YouTube =========
    try:
        rows = fetch_geography(creds, s.isoformat(), e.isoformat())
    except Exception as e:
        # Channel hợp lệ nhưng không có data → KHÔNG xác minh
        rows = []

    return {
        "start": s.isoformat(),
        "end": e.isoformat(),
        "channel": channel,
        "rows": rows,
        "availableChannels": list(CHANNEL_CREDENTIALS.keys()),
    }
