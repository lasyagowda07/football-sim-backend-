# services/data_processing.py
from __future__ import annotations

from datetime import datetime
from typing import List

import pandas as pd

from services.s3_client import s3_client


def _load_raw_data():
    """
    Load raw CSVs from our S3 abstraction (mock_s3 in local).
    """
    results = s3_client.read_csv("raw/results.csv")
    shootouts = s3_client.read_csv("raw/shootouts.csv")
    goalscorers = s3_client.read_csv("raw/goalscorers.csv")
    former_names = s3_client.read_csv("raw/former_names.csv")

    return results, shootouts, goalscorers, former_names


def _build_former_name_mapping(former_names_df: pd.DataFrame) -> dict:
    """
    Build a simple mapping from former name -> current name.

    Note:
    - The dataset already uses current names for home/away teams,
      but this keeps the logic future-proof and lets us normalize
      any stray former names if present.
    """
    if not {"current", "former"}.issubset(former_names_df.columns):
        return {}

    mapping = (
        former_names_df[["former", "current"]]
        .dropna()
        .drop_duplicates()
        .set_index("former")["current"]
        .to_dict()
    )
    return mapping


def _normalize_team_names(
    df: pd.DataFrame,
    mapping: dict,
    team_columns: List[str],
) -> pd.DataFrame:
    """
    Replace former team names with current names in the given columns.
    """
    if not mapping:
        return df

    for col in team_columns:
        if col in df.columns:
            df[col] = df[col].replace(mapping)

    return df


def _add_match_features(results: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns:
    - match_result: home_win / draw / away_win
    - goal_diff: home_score - away_score
    """
    # Ensure scores are numeric
    results["home_score"] = pd.to_numeric(results["home_score"], errors="coerce")
    results["away_score"] = pd.to_numeric(results["away_score"], errors="coerce")

    # Goal difference
    results["goal_diff"] = results["home_score"] - results["away_score"]

    # Match result
    def _result_row(row):
        if pd.isna(row["home_score"]) or pd.isna(row["away_score"]):
            return None
        if row["home_score"] > row["away_score"]:
            return "home_win"
        elif row["home_score"] < row["away_score"]:
            return "away_win"
        else:
            return "draw"

    results["match_result"] = results.apply(_result_row, axis=1)

    # Normalize neutral to boolean if needed
    if "neutral" in results.columns:
        results["neutral"] = (
            results["neutral"]
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"true": True, "false": False})
        )

    return results


def _compute_team_stats(results: pd.DataFrame) -> pd.DataFrame:
    """
    Compute basic per-team stats from the matches table.

    Returns a DataFrame with columns:
    - team
    - matches_played
    - wins
    - draws
    - losses
    - goals_for
    - goals_against
    - goal_diff
    """
    # Build home side view
    home = results[["home_team", "home_score", "away_score", "match_result"]].copy()
    home.rename(
        columns={
            "home_team": "team",
            "home_score": "goals_for",
            "away_score": "goals_against",
        },
        inplace=True,
    )
    home["is_home"] = True
    home["win"] = (home["match_result"] == "home_win").astype(int)
    home["draw"] = (home["match_result"] == "draw").astype(int)
    home["loss"] = (home["match_result"] == "away_win").astype(int)

    # Build away side view
    away = results[["away_team", "home_score", "away_score", "match_result"]].copy()
    away.rename(
        columns={
            "away_team": "team",
            "away_score": "goals_for",
            "home_score": "goals_against",
        },
        inplace=True,
    )
    away["is_home"] = False
    away["win"] = (away["match_result"] == "away_win").astype(int)
    away["draw"] = (away["match_result"] == "draw").astype(int)
    away["loss"] = (away["match_result"] == "home_win").astype(int)

    # Combine
    all_rows = pd.concat([home, away], ignore_index=True)

    # Drop rows where team is missing (just in case)
    all_rows = all_rows.dropna(subset=["team"])

    grouped = all_rows.groupby("team", as_index=False).agg(
        matches_played=("team", "count"),
        wins=("win", "sum"),
        draws=("draw", "sum"),
        losses=("loss", "sum"),
        goals_for=("goals_for", "sum"),
        goals_against=("goals_against", "sum"),
    )
    grouped["goal_diff"] = grouped["goals_for"] - grouped["goals_against"]

    return grouped


def run_processing() -> dict:
    """
    Process raw data from 'raw/' into cleaned, feature-rich tables in 'processed/'.

    Steps:
    - Load raw CSVs via s3_client
    - Normalize team names using former_names.csv
    - Add derived columns to results (match_result, goal_diff)
    - Compute basic team stats
    - Write:
      - processed/matches.csv
      - processed/teams.csv
    - Return status dict
    """
    # Load raw data
    results, shootouts, goalscorers, former_names = _load_raw_data()

    # Build and apply former name mapping
    mapping = _build_former_name_mapping(former_names)

    # Normalize team names in results & goalscorers (defensive, even if already normalized)
    results = _normalize_team_names(results, mapping, ["home_team", "away_team"])
    goalscorers = _normalize_team_names(goalscorers, mapping, ["home_team", "away_team", "team"])

    # Add match-level features
    results = _add_match_features(results)

    # Compute team-level stats
    team_stats = _compute_team_stats(results)

    # Write processed data back to S3 (mock_s3 locally)
    s3_client.write_csv(results, "processed/matches.csv")
    s3_client.write_csv(team_stats, "processed/teams.csv")

    status = {
        "status": "success",
        "records": int(len(results)),
        "teams": int(len(team_stats)),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    return status