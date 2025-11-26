from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException

from models.schemas import (
    IngestionStatus,
    ProcessingStatus,
    TrainingStatus,
    ModelRunOut,
)
from services import data_ingestion, data_processing, train_model, model_registry

router = APIRouter()


@router.post("/ingest-data", response_model=IngestionStatus)
def ingest_data():
    """
    Trigger the data ingestion step:
    - Reads CSVs from ./data
    - Uploads to mock_s3/raw (or real S3 in cloud)
    """
    try:
        result = data_ingestion.run_ingestion()
        return IngestionStatus(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e}")


@router.post("/process-data", response_model=ProcessingStatus)
def process_data():
    """
    Trigger the data processing step:
    - Reads from raw/ in S3 abstraction
    - Normalizes & enriches
    - Writes processed/matches.csv and processed/teams.csv
    """
    try:
        result = data_processing.run_processing()
        return ProcessingStatus(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {e}")


@router.post("/train-model", response_model=TrainingStatus)
def train_model_endpoint():
    """
    Trigger model training:
    - Reads processed/matches.csv
    - Trains RandomForest baseline
    - Registers ModelRun with status=ACTIVE
    """
    try:
        result = train_model.run_training()
        return TrainingStatus(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Training failed: {e}")


@router.get("/model-runs", response_model=List[ModelRunOut])
def list_model_runs_endpoint(limit: int = 20):
    """
    List recent model runs for admin UI.
    """
    runs = model_registry.list_model_runs(limit=limit)
    return [ModelRunOut.model_validate(r) for r in runs]


@router.post("/model-runs/{model_run_id}/activate")
def activate_model_run(model_run_id: str):
    """
    Mark a specific model run as ACTIVE and others as INACTIVE.
    """
    try:
        model_registry.set_active_model(model_run_id)
        return {"status": "success", "active_model_run_id": model_run_id}
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to activate model: {e}")


@router.get("/model/active", response_model=ModelRunOut | None)
def get_active_model():
    """
    Get metadata about the currently ACTIVE model run.
    """
    run = model_registry.get_latest_active_model_run()
    if not run:
        return None
    return ModelRunOut.model_validate(run)