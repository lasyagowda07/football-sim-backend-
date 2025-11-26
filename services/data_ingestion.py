# services/data_ingestion.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from services.s3_client import s3_client

# Local directory where raw Kaggle CSVs are stored for dev
DATA_DIR = Path("data")

# Expected schema for each file (minimal validation)
EXPECTED_COLUMNS: Dict[str, List[str]] = {
    "results.csv": [
        "date",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "tournament",
        "city",
        "country",
        "neutral",
    ],
    "shootouts.csv": [
        "date",
        "home_team",
        "away_team",
        "winner",
        "first_shooter",
    ],
    "goalscorers.csv": [
        "date",
        "home_team",
        "away_team",
        "team",
        "scorer",
        "own_goal",
        "penalty",
    ],
    "former_names.csv": [
        "current",
        "former",
        "start_date",
        "end_date",
    ],
}


def _validate_columns(df: pd.DataFrame, filename: str) -> None:
    """
    Ensure that the DataFrame has at least the expected columns.
    Raises ValueError if columns are missing.
    """
    expected = set(EXPECTED_COLUMNS.get(filename, []))
    if not expected:
        # No schema defined; nothing to validate
        return

    missing = expected - set(df.columns)
    if missing:
        raise ValueError(
            f"File '{filename}' is missing required columns: {', '.join(sorted(missing))}"
        )


def run_ingestion() -> dict:
    """
    Ingest local CSVs from ./data/ into our S3 abstraction (mock_s3 in local).

    Steps:
    - Read each CSV from ./data/
    - Validate required columns
    - Upload the raw file to S3 under 'raw/<filename>'
    - Return a status dict
    """
    if not DATA_DIR.exists():
        raise FileNotFoundError(
            f"Data directory not found: {DATA_DIR.resolve()}. "
            "Make sure you have the Kaggle CSVs in ./data/"
        )

    files = [
        "results.csv",
        "shootouts.csv",
        "goalscorers.csv",
        "former_names.csv",
    ]

    uploaded_keys: List[str] = []

    for fname in files:
        local_path = DATA_DIR / fname
        if not local_path.exists():
            raise FileNotFoundError(
                f"Expected file not found: {local_path.resolve()}. "
                "Download from Kaggle and place it in ./data/."
            )

        # Load CSV to validate schema
        df = pd.read_csv(local_path)
        _validate_columns(df, fname)

        # Upload the original raw file to (mock) S3
        s3_key = f"raw/{fname}"
        s3_client.upload_file(local_path, s3_key)
        uploaded_keys.append(s3_key)

    status = {
        "status": "success",
        "files": uploaded_keys,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    return status


# Optional: placeholder for Kaggle-based ingestion (future)
def run_kaggle_ingestion() -> dict:
    """
    Placeholder for future: use kagglehub to directly pull data instead of manual CSVs.

    For now, this is just a stub so you remember where to plug it in later.
    """
    raise NotImplementedError(
        "Kaggle-based ingestion not implemented yet. "
        "Use run_ingestion() with local CSVs in ./data/ for now."
    )