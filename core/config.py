import os
from pathlib import Path

from dotenv import load_dotenv

# Base directory of the project (folder where main.py lives)
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env from project root
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)


class Settings:
    def __init__(self) -> None:
        # General environment
        self.ENV: str = os.getenv("ENV", "local")

        # Database URL (SQLite by default)
        # Example: sqlite:///./app.db
        self.DB_URL: str = os.getenv("DB_URL", "sqlite:///./app.db")

        # S3 / storage
        self.S3_BUCKET: str = os.getenv("S3_BUCKET", "local-mock-bucket")

        # For local mock S3 (a folder that behaves like S3)
        self.MOCK_S3_ROOT: str = os.getenv(
            "MOCK_S3_ROOT",
            str(BASE_DIR / "mock_s3")
        )


settings = Settings()