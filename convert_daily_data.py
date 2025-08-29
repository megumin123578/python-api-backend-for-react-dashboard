import csv, io, os, sys
from datetime import datetime, timedelta

input_file = r"C:\Users\Admin\Documents\dev\20_8_2025\reports\credentials_dtienbac_kenh2\Daily_summary.csv"
output_file = r"C:\Users\Admin\Documents\dev\dashboard\react-dashboard\src\data\Daily.js"

def sniff_delimiter(sample_bytes: bytes, fallback=","):
    try:
        dialect = csv.Sniffer().sniff(sample_bytes.decode("utf-8", errors="ignore"))
        return dialect.delimiter
    except Exception:
        return fallback

# Đọc file (bỏ BOM, tự dò delimiter)
if not os.path.exists(input_file):
    print(f"Không tìm thấy file: {input_file}")
    sys.exit(1)

with open(input_file, "rb") as fb:
    raw = fb.read()
if not raw.strip():
    print("CSV rỗng.")
    sys.exit(1)

sample = raw[:4096]
delimiter = sniff_delimiter(sample)
text = raw.decode("utf-8-sig", errors="replace")

# Tạo DictReader
f = io.StringIO(text)
reader = csv.DictReader(f, delimiter=delimiter)
if not reader.fieldnames:
    print("Không đọc được header. Kiểm tra dòng đầu của CSV.")
    sys.exit(1)

# Chuẩn hoá header
norm_map = {h: h.strip() for h in reader.fieldnames}
fieldnames = [norm_map[h] for h in reader.fieldnames]

rows = []
for row in reader:
    if not any(v.strip() for v in row.values() if isinstance(v, str)):
        continue
    fixed = {norm_map[k]: v.strip() if isinstance(v, str) else v for k, v in row.items()}
    rows.append(fixed)

print("== DIAGNOSTIC ==")
print("Delimiter:", repr(delimiter))
print("Headers CSV:", fieldnames)
print("Số dòng dữ liệu:", len(rows))
if rows[:1]:
    print("Mẫu dòng đầu:", rows[0])

# --- BƯỚC MỚI: tự bù ngày nếu có cột 'day' ---
def try_parse_date(s: str):
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

if "day" in fieldnames and rows:
    # get min/max day in data
    parsed_days = [try_parse_date(r["day"]) for r in rows if r.get("day")]
    parsed_days = [d for d in parsed_days if d is not None]
    if parsed_days:
        start = min(parsed_days)
        end = max(parsed_days)

        have = {try_parse_date(r["day"]) for r in rows if r.get("day")}
        have.discard(None)

        numeric_fields = [
            "views",
            "estimatedMinutesWatched",
            "averageViewDuration",
            "averageViewPercentage",
            "engagedViews",
            "subscribersGained",
            "subscribersLost",
            "likes",
            "shares",
            "comments",
            "impressions",
            "impressionsClickThroughRate",
        ]

       
        cur = start
        while cur <= end:
            if cur not in have:
                # tạo hàng trống cho ngày thiếu
                empty_row = {"day": cur.strftime("%Y-%m-%d")}
                for k in fieldnames:
                    if k == "day":
                        continue
                    elif k in numeric_fields:
                        empty_row[k] = "0"
                    else:
                        empty_row[k] = ""
                rows.append(empty_row)
            cur += timedelta(days=1)

        #sort day
        def day_key(r):
            d = try_parse_date(r.get("day", ""))
            return d or datetime.min.date()
        rows.sort(key=day_key)


os.makedirs(os.path.dirname(output_file), exist_ok=True)
with open(output_file, "w", encoding="utf-8") as out:
    out.write("export const DailyData = [\n")
    for r in rows:
        insight = r.get("insightTrafficSourceType", "")
        day = r.get("day", None)

        def to_int(v, default=0):
            try: return int(float(v))
            except: return default

        def to_float(v, default=0.0):
            try: return float(v)
            except: return default

        # các trường sẵn có
        views = to_int(r.get("views", 0))
        emw   = to_int(r.get("estimatedMinutesWatched", 0))
        avd   = to_int(r.get("averageViewDuration", 0))
        avp   = to_float(r.get("averageViewPercentage", 0))
        eng   = to_int(r.get("engagedViews", 0))

        # ===== BỔ SUNG 5 TRƯỜNG BẠN BẢO THIẾU =====
        subs_gained = to_int(r.get("subscribersGained", 0))
        subs_lost   = to_int(r.get("subscribersLost", 0))
        likes       = to_int(r.get("likes", 0))
        shares      = to_int(r.get("shares", 0))
        comments    = to_int(r.get("comments", 0))
        # ===========================================

        out.write("  {\n")
        if day is not None:
            out.write(f"    day: \"{day}\",\n")
        if insight:
            out.write(f"    insightTrafficSourceType: \"{insight}\",\n")
        out.write(f"    views: {views},\n")
        out.write(f"    estimatedMinutesWatched: {emw},\n")
        out.write(f"    averageViewDuration: {avd},\n")
        out.write(f"    averageViewPercentage: {avp:.2f},\n")
        out.write(f"    engagedViews: {eng},\n")

        # ===== GHI RA FILE JSON =====
        out.write(f"    subscribersGained: {subs_gained},\n")
        out.write(f"    subscribersLost: {subs_lost},\n")
        out.write(f"    likes: {likes},\n")
        out.write(f"    shares: {shares},\n")
        out.write(f"    comments: {comments},\n")
        # ============================

        out.write("  },\n")
    out.write("];\n")

print(f"Xuất xong sang {output_file}")
