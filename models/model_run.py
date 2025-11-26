from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, DateTime, String
from sqlalchemy.types import JSON

from core.db import Base


class ModelRun(Base):
    __tablename__ = "model_runs"

    # Store UUID as string for SQLite compatibility
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Path to model file in "S3" (or mock_s3 locally)
    model_s3_path = Column(String, nullable=False)

    # Status of this model run: ACTIVE / INACTIVE / FAILED
    status = Column(String(20), nullable=False, default="ACTIVE")

    # Metrics as JSON (e.g. {"accuracy": 0.85, "log_loss": 0.42})
    metrics = Column(JSON, nullable=True)

    # Optional notes/description about this run
    notes = Column(String, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<ModelRun(id={self.id}, status={self.status}, "
            f"created_at={self.created_at}, model_s3_path={self.model_s3_path})>"
        )