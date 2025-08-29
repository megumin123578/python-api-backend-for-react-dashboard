from googleapiclient.discovery import build
import pickle
import csv

TOKEN = 'token/dtienbac.pickle'
START_DATE = '2025-07-15'
END_DATE   = '2025-08-14'

with open(TOKEN, 'rb') as f:
    creds = pickle.load(f)

ya = build('youtubeAnalytics', 'v2', credentials=creds)

# 1) Theo traffic source (gần với Reach: nguồn tìm thấy nội dung)
resp_src = ya.reports().query(
    ids="channel==MINE",
    startDate=START_DATE,
    endDate=END_DATE,
    metrics="views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
    dimensions="insightTrafficSourceType",
    sort="-views"
).execute()

print("By Traffic Source:")
for row in resp_src.get('rows', []):
    src, views, watch_min, avg_dur_sec, avg_pct = row
    print(f"{src:25} | views={views} | watch_min={watch_min} | avg_dur_sec={avg_dur_sec} | avg_view%={round(avg_pct*100,2)}")

# 2) Theo ngày (để xem 'reach-ish' theo thời gian)
resp_day = ya.reports().query(
    ids="channel==MINE",
    startDate=START_DATE,
    endDate=END_DATE,
    metrics="views,estimatedMinutesWatched,averageViewDuration",
    dimensions="day",
    sort="day"
).execute()

with open('reach_like_daily.csv', 'w', newline='', encoding='utf-8-sig') as f:
    w = csv.writer(f)
    w.writerow(['day','views','watch_minutes','avg_view_duration_sec'])
    for d, v, m, avg in resp_day.get('rows', []):
        w.writerow([d, v, m, avg])
print("Saved: reach_like_daily.csv")
