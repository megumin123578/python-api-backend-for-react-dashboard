# db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

PG_URL = os.getenv(
    "PG_URL",
    "postgresql+psycopg2://postgres:V!etdu1492003@localhost:5432/analytics"
)

# Engine
engine = create_engine(
    PG_URL,
    pool_pre_ping=True,
    future=True
)

# Session Factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Dependency để inject vào router FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
