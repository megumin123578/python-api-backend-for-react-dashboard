from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url
import os

pg_env = os.getenv("PG_URL")

if pg_env:
    # Phân tích URL để debug nhanh, không lộ mật khẩu
    try:
        u = make_url(pg_env)
        print("[PG_URL env] FOUND")
        print("[Parse]", u.drivername, u.host, u.port, u.database)
        sa_url = u
    except Exception as e:
        raise ValueError(f"PG_URL không hợp lệ: {e}")
else:
    print("[PG_URL env] None -> dùng fallback")
    # Tránh lỗi URL-encode bằng cách tạo URL từng thành phần
    sa_url = URL.create(
        drivername="postgresql+psycopg2",  # hoặc "postgresql+psycopg" nếu xài psycopg3
        username="postgres",
        password="V!etdu1492003",
        host="localhost",
        port=5432,
        database="analytics",
    )
    print("[Parse]", sa_url.drivername, sa_url.host, sa_url.port, sa_url.database)

eng = create_engine(sa_url, pool_pre_ping=True, future=True)
with eng.begin() as c:
    print(c.scalar(text("SELECT 1")))
