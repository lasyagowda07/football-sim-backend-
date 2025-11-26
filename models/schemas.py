from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field


# -----------------------------
# Simulation-related schemas
# -----------------------------
class SimulationRequest(BaseModel):
    """
    Payload sent from the UI to request a tournament simulation.
    """
    teams: List[str] = Field(..., description="List of team names participating in the tournament.")
    n_runs: int = Field(..., gt=0, description="Number of Monte Carlo simulation runs.")


class TeamProbability(BaseModel):
    """
    Per-team result in a simulated tournament.
    """
    team: str
    win_prob: float = Field(..., ge=0.0, le=1.0)
    final_prob: Optional[float] = Field(None, ge=0.0, le=1.0)
    semi_prob: Optional[float] = Field(None, ge=0.0, le=1.0)
    wins: Optional[int] = None
    finals: Optional[int] = None
    semis: Optional[int] = None


class SimulationResponse(BaseModel):
    """
    Response returned to the UI after running a tournament simulation.
    """
    simulation_id: str
    results: List[TeamProbability]


# -----------------------------
# Admin: ingestion / processing / training
# -----------------------------
class IngestionStatus(BaseModel):
    """
    Status returned by the data ingestion step.
    """
    status: str
    files: List[str]
    timestamp: str


class ProcessingStatus(BaseModel):
    """
    Status returned by the data processing step.
    """
    status: str
    records: int
    teams: int
    timestamp: str


class TrainingStatus(BaseModel):
    """
    Status returned by the model training step.
    """
    status: str
    model_run_id: str
    model_s3_path: str
    metrics: Dict[str, Union[float, int]]
    timestamp: str


# -----------------------------
# ModelRun listing for admin UI
# -----------------------------
class ModelRunOut(BaseModel):
    """
    Read-only representation of a ModelRun row for API responses.
    """
    id: str
    created_at: datetime
    model_s3_path: str
    status: str
    metrics: Optional[Dict[str, Union[float, int]]] = None
    notes: Optional[str] = None

    class Config:
        from_attributes = True  # pydantic v2: allows constructing from ORM objects