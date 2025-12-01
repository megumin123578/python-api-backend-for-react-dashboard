from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def fetch_geography(credentials, start_date: str, end_date: str):
    yta = build("youtubeAnalytics", "v2", credentials=credentials)

    metrics = ",".join([
        "views",
        "estimatedMinutesWatched",
        "engagedViews",
        "averageViewDuration",
        "averageViewPercentage",
    ])

    q = {
        "ids": "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": "country",
        "metrics": metrics,
        "sort": "-views"
    }

    try:
        resp = yta.reports().query(**q).execute() or {}
    except HttpError as e:
        print("[ERROR] Geography query failed:", e)
        return []

    rows = resp.get("rows", [])
    headers = [h["name"] for h in resp.get("columnHeaders", [])]

    # Map column indexes
    idx = {h: i for i, h in enumerate(headers)}

    out = []
    for r in rows:
        out.append({
            "country": r[idx["country"]],
            "views": int(r[idx["views"]] or 0),
            "estimatedMinutesWatched": int(r[idx["estimatedMinutesWatched"]] or 0),
            "engagedViews": int(r[idx["engagedViews"]] or 0),
            "averageViewDuration": int(r[idx["averageViewDuration"]] or 0),
            "averageViewPercentage": float(r[idx["averageViewPercentage"]] or 0),
        })

    return out
