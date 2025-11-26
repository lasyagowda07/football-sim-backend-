from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from core.config import settings

# Base class for our ORM models
Base = declarative_base()

# For SQLite, we need check_same_thread=False
connect_args = {}
if settings.DB_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

# Create the SQLAlchemy engine
engine = create_engine(
    settings.DB_URL,
    connect_args=connect_args,
    future=True,
)

# Create a configured "Session" class
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


# Dependency for FastAPI routes (you can use this later)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()