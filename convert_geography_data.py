import csv, io, os, sys, hashlib

input_file = r"C:\Users\Admin\Documents\dev\dashboard\python_backend\reports\credentials_dtienbac_kenh2/Geography_by_country.csv"
output_file = r"C:\Users\Admin\Documents\dev\dashboard\react-dashboard\src\data\Geography.js"

def sniff_delimiter(sample_bytes: bytes, fallback=","):
    try:
        dialect = csv.Sniffer().sniff(sample_bytes.decode("utf-8", errors="ignore"))
        return dialect.delimiter
    except Exception:
        return fallback

def hsl_from_text(s: str) -> str:
    h = int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16) % 360
    return f"hsl({h}, 70%, 50%)"

def to_number(s, default=0):
    try:
        return float((s or "0").replace(",", ""))
    except:
        return default

# đọc CSV
if not os.path.exists(input_file):
    print(f"Không tìm thấy file: {input_file}")
    sys.exit(1)

with open(input_file, "rb") as fb:
    raw = fb.read()
if not raw.strip():
    print("CSV rỗng.")
    sys.exit(1)

delim = sniff_delimiter(raw[:4096])
text = raw.decode("utf-8-sig", errors="replace")
reader = csv.DictReader(io.StringIO(text), delimiter=delim)
if not reader.fieldnames:
    print("Không đọc được header.")
    sys.exit(1)

rows = [ {k.strip(): (v or "").strip() for k,v in r.items()} for r in reader if any((v or "").strip() for v in r.values()) ]

# build output
items = []
for r in rows:
    idv = (r.get("country") or r.get("Country") or r.get("countryCode") or "").strip()
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
    }
    items.append(item)


# ghi file JS
os.makedirs(os.path.dirname(output_file), exist_ok=True)
with open(output_file, "w", encoding="utf-8") as out:
    out.write("export const geography = [\n")
    for it in items:
        out.write("  {\n")
        for k,v in it.items():
            if isinstance(v,(int,float)):
                out.write(f"    {k}: {v},\n")
            else:
                out.write(f"    {k}: \"{v}\",\n")
        out.write("  },\n")
    out.write("];\n")

print(f"Xuất xong sang {output_file}")
