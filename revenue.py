import os
import csv
import pickle
from datetime import datetime
from typing import Tuple, Dict, Any, Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========================
# Config
# ========================
SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.monetary.readonly",  # bắt buộc để đọc revenue
]

TOKEN_FILE = "token/abc.pickle"
OUTPUT_FILE = "youtube_revenue.csv"

# Nếu có CONTENT_OWNER_ID -> dùng owner mode
CONTENT_OWNER_ID = os.environ.get("CONTENT_OWNER_ID", "").strip()
IS_OWNER_MODE = bool(CONTENT_OWNER_ID)


# ========================
# Auth (load từ pickle + refresh)
# ========================
def get_credentials() -> Credentials:
    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError(f"Không tìm thấy token pickle: {TOKEN_FILE}")

    with open(TOKEN_FILE, "rb") as token:
        creds: Credentials = pickle.load(token)

    # Refresh nếu hết hạn và có refresh_token
    if getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
        creds.refresh(Request())

    # Kiểm tra scope (đặc biệt monetary)
    granted = set(getattr(creds, "scopes", []) or [])
    required = set(SCOPES)
    if not required.issubset(granted):
        missing = ", ".join(sorted(required - granted))
        raise PermissionError(
            "Token hiện tại thiếu scope bắt buộc để đọc doanh thu.\n"
            f"Thiếu: {missing}\n"
            "Hãy tạo lại token với scope 'yt-analytics.monetary.readonly'."
        )

    return creds


# ========================
# API call
# ========================
def _ids_params() -> Tuple[str, Dict[str, Any]]:
    """
    Trả về cặp (ids, extra_params) cho query().
    - channel==MINE (mặc định)
    - Hoặc contentOwner==<ID> + onBehalfOfContentOwner nếu có CONTENT_OWNER_ID
    """
    if IS_OWNER_MODE:
        return f"contentOwner=={CONTENT_OWNER_ID}", {"onBehalfOfContentOwner": CONTENT_OWNER_ID}
    return "channel==MINE", {}


def get_revenue(start_date: str, end_date: str, currency: str = "USD") -> Dict[str, Any]:
    creds = get_credentials()
    yta = build("youtubeAnalytics", "v2", credentials=creds)

    ids, extra = _ids_params()

    # dimensions=day để lấy theo ngày; có thể bỏ để lấy tổng period
    req = {
        "ids": ids,
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "estimatedRevenue",
        "dimensions": "day",
        "currency": currency,  # optional nhưng nên set rõ
        **extra,
    }

    return yta.reports().query(**req).execute()


# ========================
# CSV I/O
# ========================
def save_to_csv(response: Dict[str, Any], output_file: str) -> None:
    headers = ["date", "estimatedRevenue"]
    rows = response.get("rows", []) or []

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to {output_file}")


# ========================
# Main
# ========================
if __name__ == "__main__":
    # Ví dụ: từ đầu năm đến hôm nay
    start_date = "2025-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")

    try:
        resp = get_revenue(start_date, end_date, currency="USD")
        save_to_csv(resp, OUTPUT_FILE)

        for row in resp.get("rows", []) or []:
            print("Date:", row[0], "Revenue:", row[1])

    except PermissionError as e:
        # Thiếu scope monetary hoặc token sai phạm vi
        print("Permission error:", e)

    except HttpError as e:
        print("HTTP Error:", getattr(e, "status_code", "unknown"))
        print((getattr(e, "content", b"") or b"")[:600])

    except Exception as e:
        print("Unexpected error:", repr(e))
