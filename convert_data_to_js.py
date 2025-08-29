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

if __name__ == "__main__":
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
