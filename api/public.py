from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException

from core.db import SessionLocal
from models.schemas import (
    SimulationRequest,
    SimulationResponse,
    TeamProbability,
)
from models.simulation_run import SimulationRun
from services import stats, simulation

router = APIRouter()


@router.get("/teams", response_model=List[str])
def get_teams_endpoint():
    """
    Return list of team names for the UI to populate dropdowns.
    """
    try:
        return stats.get_teams()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load teams: {e}")


@router.post("/simulate-tournament", response_model=SimulationResponse)
def simulate_tournament_endpoint(payload: SimulationRequest):
    """
    Run a tournament simulation with the ACTIVE model.

    - Takes list of teams and number of runs
    - Calls simulation.simulate_tournament(...)
    - Returns SimulationResponse with probabilities per team
    """
    try:
        result = simulation.simulate_tournament(
            teams=payload.teams,
            n_runs=payload.n_runs,
            neutral=False,  # you can parameterize this later
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Simulation failed: {e}")

    # result["summary"] is {team: {...stats...}, ...}
    team_probs = [
        TeamProbability(team=team, **stats_dict)
        for team, stats_dict in result["summary"].items()
    ]

    return SimulationResponse(
        simulation_id=result["simulation_id"],
        results=team_probs,
    )


@router.get("/simulation/{simulation_id}", response_model=SimulationResponse)
def get_simulation(simulation_id: str):
    """
    Fetch a previously saved SimulationRun and return it
    as a SimulationResponse.
    """
    db = SessionLocal()
    try:
        sim: SimulationRun | None = (
            db.query(SimulationRun)
            .filter(SimulationRun.id == simulation_id)
            .first()
        )
        if sim is None:
            raise HTTPException(status_code=404, detail="Simulation not found")

        # sim.results is the summary dict: {team: {...stats...}, ...}
        team_probs = [
            TeamProbability(team=team, **stats_dict)
            for team, stats_dict in sim.results.items()
        ]

        return SimulationResponse(
            simulation_id=sim.id,
            results=team_probs,
        )
    finally:
        db.close()