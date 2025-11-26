from __future__ import annotations

from typing import Dict, List, Any

import numpy as np
import pandas as pd

from core.db import SessionLocal
from models.simulation_run import SimulationRun
from services.model_registry import load_active_model, get_latest_active_model_run


def _is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0


def simulate_match(home_team: str, away_team: str, neutral: bool = False) -> Dict[str, float]:
    """
    Use the ACTIVE model to get probabilities for a single match.

    Returns dict:
      {
        "home_win": p_home,
        "draw": p_draw,
        "away_win": p_away
      }
    """
    artifact = load_active_model()
    model = artifact["model"]
    label_encoder = artifact["label_encoder"]

    # Encode teams
    home_enc = label_encoder.transform([str(home_team)])[0]
    away_enc = label_encoder.transform([str(away_team)])[0]

    X = pd.DataFrame(
        {
            "home_team_enc": [home_enc],
            "away_team_enc": [away_enc],
            "neutral": [int(bool(neutral))],
        }
    )

    proba = model.predict_proba(X)[0]  # (n_classes,)
    classes = model.classes_

    probs = {cls: float(p) for cls, p in zip(classes, proba)}

    # Ensure all keys exist
    for k in ("home_win", "draw", "away_win"):
        probs.setdefault(k, 0.0)

    return probs


def _simulate_single_tournament(
    teams: List[str],
    rng: np.random.Generator,
    neutral: bool = False,
) -> Dict[str, Dict[str, int]]:
    """
    Run a single knockout tournament simulation.

    Returns stats for this single run:
      {team: {"wins": 0/1, "finals": 0/1, "semis": 0/1}}
    """
    stats = {team: {"wins": 0, "finals": 0, "semis": 0} for team in teams}

    current_round = teams[:]
    rng.shuffle(current_round)

    while len(current_round) > 1:
        n = len(current_round)

        # Mark semifinalists
        if n == 4:
            for t in current_round:
                stats[t]["semis"] += 1

        # Mark finalists
        if n == 2:
            for t in current_round:
                stats[t]["finals"] += 1

        next_round: List[str] = []

        for i in range(0, n, 2):
            home = current_round[i]
            away = current_round[i + 1]

            probs = simulate_match(home, away, neutral=neutral)

            labels = np.array(["home_win", "draw", "away_win"])
            probs_arr = np.array(
                [probs["home_win"], probs["draw"], probs["away_win"]],
                dtype=float,
            )

            total = probs_arr.sum()
            if total <= 0:
                probs_arr = np.array([1.0, 0.0, 0.0])
            else:
                probs_arr = probs_arr / total

            outcome = rng.choice(labels, p=probs_arr)

            if outcome == "home_win":
                winner = home
            elif outcome == "away_win":
                winner = away
            else:  # draw
                winner = rng.choice([home, away])

            next_round.append(winner)

        current_round = next_round

    champion = current_round[0]
    stats[champion]["wins"] += 1

    return stats


def simulate_tournament(
    teams: List[str],
    n_runs: int,
    neutral: bool = False,
) -> Dict[str, Any]:
    """
    Simulate a knockout tournament with the ACTIVE model and
    persist the result in SimulationRun table.

    Returns:
      {
        "status": "success",
        "simulation_id": ...,
        "n_runs": ...,
        "model_run_id": ...,
        "summary": {
           team: {
             "wins": ...,
             "finals": ...,
             "semis": ...,
             "win_prob": ...,
             "final_prob": ...,
             "semi_prob": ...
           },
           ...
        }
      }
    """
    if len(teams) < 2:
        raise ValueError("Need at least 2 teams to simulate a tournament.")

    if not _is_power_of_two(len(teams)):
        raise ValueError(
            f"Number of teams must be a power of two for simple knockout. Got {len(teams)}."
        )

    # Remove duplicates but preserve order
    seen = set()
    unique_teams: List[str] = []
    for t in teams:
        if t not in seen:
            seen.add(t)
            unique_teams.append(t)

    # Ensure there is an ACTIVE model (raises if none)
    artifact = load_active_model()
    _ = artifact["model"]  # just to ensure loaded

    aggregate = {
        team: {"wins": 0, "finals": 0, "semis": 0}
        for team in unique_teams
    }

    rng = np.random.default_rng(seed=42)

    for _ in range(n_runs):
        single_stats = _simulate_single_tournament(unique_teams, rng, neutral=neutral)
        for team, s in single_stats.items():
            aggregate[team]["wins"] += s["wins"]
            aggregate[team]["finals"] += s["finals"]
            aggregate[team]["semis"] += s["semis"]

    # Convert counts to probabilities
    summary = {}
    for team, s in aggregate.items():
        summary[team] = {
            "wins": int(s["wins"]),
            "finals": int(s["finals"]),
            "semis": int(s["semis"]),
            "win_prob": s["wins"] / n_runs,
            "final_prob": s["finals"] / n_runs,
            "semi_prob": s["semis"] / n_runs,
        }

    # Link to active model run if exists
    active_model_run = get_latest_active_model_run()
    model_run_id = active_model_run.id if active_model_run is not None else None

    # Save to SimulationRun table
    db = SessionLocal()
    try:
        sim_run = SimulationRun(
            teams=unique_teams,
            n_runs=n_runs,
            results=summary,
            model_run_id=model_run_id,
            notes="Knockout tournament simulation",
        )
        db.add(sim_run)
        db.commit()
        db.refresh(sim_run)
        simulation_id = sim_run.id
    finally:
        db.close()

    return {
        "status": "success",
        "simulation_id": simulation_id,
        "n_runs": n_runs,
        "model_run_id": model_run_id,
        "summary": summary,
    }