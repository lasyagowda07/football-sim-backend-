from pathlib import Path
import pandas as pd

from services.s3_client import s3_client

def run_ingestion() -> dict:
    # local source files for now
    data_dir = Path("data")
    files = [
        "results.csv",
        "shootouts.csv",
        "goalscorers.csv",
        "former_names.csv",
    ]

    uploaded = []
    for fname in files:
        local_path = data_dir / fname
        s3_key = f"raw/{fname}"
        s3_client.upload_file(local_path, s3_key)
        uploaded.append(s3_key)

    return {
        "status": "success",
        "uploaded": uploaded,
    }