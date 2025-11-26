from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from sqlalchemy.types import JSON

from core.db import Base


class SimulationRun(Base):
    __tablename__ = "simulation_runs"

    # UUID stored as string for SQLite compatibility
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # List of team names for this simulation run
    teams = Column(JSON, nullable=False)

    # Number of Monte Carlo runs
    n_runs = Column(Integer, nullable=False)

    # Results summary per team:
    # { "Brazil": {"wins": ..., "finals": ..., "semis": ..., "win_prob": ...}, ... }
    results = Column(JSON, nullable=False)

    # Optional: which model_run produced this simulation
    model_run_id = Column(String(36), ForeignKey("model_runs.id"), nullable=True)

    # Free-text notes
    notes = Column(String, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<SimulationRun(id={self.id}, n_runs={self.n_runs}, "
            f"created_at={self.created_at}, model_run_id={self.model_run_id})>"
        )