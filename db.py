# db.py
import os
from sqlalchemy import create_engine

PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:V!etdu1492003@localhost:5432/analytics")
engine = create_engine(PG_URL, pool_pre_ping=True, future=True)
