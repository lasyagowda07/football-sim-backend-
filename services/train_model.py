# services/train_model.py
from __future__ import annotations

import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, log_loss
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from core.db import SessionLocal
from models.model_run import ModelRun
from services.s3_client import s3_client


def _load_processed_matches() -> pd.DataFrame:
    """
    Load the processed matches table from S3 (mock_s3 in local).
    """
    df = s3_client.read_csv("processed/matches.csv")
    return df


def _build_features_and_target(matches: pd.DataFrame):
    """
    Build simple numeric features and target from the matches DataFrame.

    Features:
      - home_team_enc: label-encoded home_team
      - away_team_enc: label-encoded away_team
      - neutral: 0/1 flag

    Target:
      - match_result: 'home_win', 'draw', 'away_win'
    """

    # Drop rows where match_result is missing
    matches = matches.dropna(subset=["match_result"])

    # Ensure we have the necessary columns
    required_cols = {"home_team", "away_team", "neutral", "match_result"}
    missing = required_cols - set(matches.columns)
    if missing:
        raise ValueError(f"Processed matches are missing columns: {', '.join(sorted(missing))}")

    # Convert neutral to numeric (0/1)
    neutral = matches["neutral"].fillna(False).astype(int)

    # Label-encode teams
    le = LabelEncoder()
    # Fit on combined home + away teams so they share the same space
    all_teams = pd.concat([matches["home_team"], matches["away_team"]], ignore_index=True)
    le.fit(all_teams.astype(str))

    home_enc = le.transform(matches["home_team"].astype(str))
    away_enc = le.transform(matches["away_team"].astype(str))

    X = pd.DataFrame(
        {
            "home_team_enc": home_enc,
            "away_team_enc": away_enc,
            "neutral": neutral,
        }
    )

    y = matches["match_result"].astype(str)

    return X, y, le


def run_training() -> Dict[str, Any]:
    """
    Train a baseline match-outcome model and register it.

    Steps:
    - Load processed/matches.csv from S3
    - Build features + target
    - Train RandomForestClassifier
    - Compute metrics (accuracy, log_loss)
    - Serialize model + label encoder to a pickle
    - Upload pickle to S3 under models/model_<timestamp>.pkl
    - Insert ModelRun row in DB:
        - model_s3_path, metrics, status="ACTIVE" (set others INACTIVE)
    - Return summary dict
    """
    # 1. Load data
    matches = _load_processed_matches()

    if matches.empty:
        raise ValueError("No processed matches found to train on.")

    # 2. Features and target
    X, y, label_encoder = _build_features_and_target(matches)

    # 3. Train/validation split
    X_train, X_val, y_train, y_val = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    # 4. Model training (baseline RandomForest)
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=None,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # 5. Evaluation
    y_pred = model.predict(X_val)
    acc = accuracy_score(y_val, y_pred)

    # For log_loss we need predicted probabilities
    y_proba = model.predict_proba(X_val)
    # model.classes_ aligns with columns of y_proba
    ll = log_loss(y_val, y_proba, labels=model.classes_)

    metrics = {
        "accuracy": float(acc),
        "log_loss": float(ll),
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
    }

    # 6. Serialize model + label encoder together
    artifact = {
        "model": model,
        "label_encoder": label_encoder,
    }

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    local_models_dir = Path("local_models")
    local_models_dir.mkdir(parents=True, exist_ok=True)
    local_model_path = local_models_dir / f"model_{timestamp}.pkl"

    with local_model_path.open("wb") as f:
        pickle.dump(artifact, f)

    # 7. Upload to S3 (mock_s3 in local)
    s3_key = f"models/model_{timestamp}.pkl"
    s3_client.upload_file(local_model_path, s3_key)

    model_s3_path = s3_key

    # 8. Register in DB (ModelRun)
    db = SessionLocal()
    try:
        # Mark existing ACTIVE models as INACTIVE
        db.query(ModelRun).filter(ModelRun.status == "ACTIVE").update(
            {"status": "INACTIVE"}
        )

        model_run = ModelRun(
            model_s3_path=model_s3_path,
            status="ACTIVE",
            metrics=metrics,
            notes="Baseline RandomForest with team label-encoding and neutral flag.",
        )
        db.add(model_run)
        db.commit()
        db.refresh(model_run)
        model_run_id = model_run.id
    finally:
        db.close()

    # 9. Return summary
    return {
        "status": "success",
        "model_run_id": model_run_id,
        "model_s3_path": model_s3_path,
        "metrics": metrics,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }