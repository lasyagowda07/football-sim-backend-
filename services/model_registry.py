# services/model_registry.py
from __future__ import annotations

import os
import pickle
from pathlib import Path
from typing import Any, List, Optional

from core.db import SessionLocal
from models.model_run import ModelRun
from services.s3_client import s3_client

# Module-level cache for the active model artifact
_ACTIVE_MODEL_ARTIFACT: Optional[Any] = None
_ACTIVE_MODEL_RUN_ID: Optional[str] = None


def list_model_runs(limit: int = 20) -> List[ModelRun]:
    """
    Return the most recent model runs, ordered by created_at descending.
    """
    db = SessionLocal()
    try:
        runs = (
            db.query(ModelRun)
            .order_by(ModelRun.created_at.desc())
            .limit(limit)
            .all()
        )
        return runs
    finally:
        db.close()


def get_latest_active_model_run() -> Optional[ModelRun]:
    """
    Return the most recent ACTIVE model run, or None if there isn't one.
    """
    db = SessionLocal()
    try:
        run = (
            db.query(ModelRun)
            .filter(ModelRun.status == "ACTIVE")
            .order_by(ModelRun.created_at.desc())
            .first()
        )
        return run
    finally:
        db.close()


def set_active_model(model_run_id: str) -> None:
    """
    Mark the given model_run_id as ACTIVE and set all others to INACTIVE.
    Also clears the in-memory cache so the new model will load next time.
    """
    global _ACTIVE_MODEL_ARTIFACT, _ACTIVE_MODEL_RUN_ID

    db = SessionLocal()
    try:
        # First set all ACTIVE to INACTIVE
        db.query(ModelRun).filter(ModelRun.status == "ACTIVE").update(
            {"status": "INACTIVE"}
        )

        # Then set the chosen one to ACTIVE
        run = db.query(ModelRun).filter(ModelRun.id == model_run_id).first()
        if run is None:
            raise ValueError(f"ModelRun with id={model_run_id} not found.")

        run.status = "ACTIVE"
        db.add(run)
        db.commit()
    finally:
        db.close()

    # Clear cache
    _ACTIVE_MODEL_ARTIFACT = None
    _ACTIVE_MODEL_RUN_ID = None


def _download_model_to_local(model_s3_path: str) -> Path:
    """
    Download the model artifact from S3 to a local temp folder and return its path.
    """
    local_dir = Path("local_models_cache")
    local_dir.mkdir(parents=True, exist_ok=True)

    filename = os.path.basename(model_s3_path)
    local_path = local_dir / filename

    # Download from S3
    s3_client.download_file(model_s3_path, local_path)

    return local_path


def load_active_model() -> Any:
    """
    Load the currently ACTIVE model run's artifact (model + label encoder).

    - If already cached in memory, return that.
    - Otherwise:
        - Find latest ACTIVE ModelRun
        - Download its model file from S3
        - Unpickle
        - Cache in module-level variable
    """
    global _ACTIVE_MODEL_ARTIFACT, _ACTIVE_MODEL_RUN_ID

    # Return cached version if we already loaded it
    if _ACTIVE_MODEL_ARTIFACT is not None and _ACTIVE_MODEL_RUN_ID is not None:
        return _ACTIVE_MODEL_ARTIFACT

    # Find latest ACTIVE model run
    active_run = get_latest_active_model_run()
    if active_run is None:
        raise RuntimeError("No ACTIVE model run found. Train a model first.")

    # Download model file from S3 (or mock_s3)
    local_model_path = _download_model_to_local(active_run.model_s3_path)

    # Unpickle artifact (expected to be dict with model + label_encoder)
    with local_model_path.open("rb") as f:
        artifact = pickle.load(f)

    # Cache
    _ACTIVE_MODEL_ARTIFACT = artifact
    _ACTIVE_MODEL_RUN_ID = active_run.id

    return artifact