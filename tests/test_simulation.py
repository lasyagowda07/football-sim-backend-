from services.data_ingestion import run_ingestion
from services.data_processing import run_processing
from services.train_model import run_training
from services.stats import get_teams
from services.simulation import simulate_tournament
from core.db import SessionLocal
from models.simulation_run import SimulationRun


if __name__ == "__main__":
    print("Step 1: Ingestion...")
    print(run_ingestion())

    print("\nStep 2: Processing...")
    print(run_processing())

    print("\nStep 3: Training...")
    print(run_training())

    print("\nStep 4: Getting teams...")
    teams = get_teams()
    print(f"Total teams: {len(teams)}")

    selected = teams[:8]
    print("Selected teams:", selected)

    print("\nStep 5: Simulating tournament (n_runs=100)...")
    sim_result = simulate_tournament(selected, n_runs=100)
    print("simulation_id:", sim_result["simulation_id"])
    for team, stats in sim_result["summary"].items():
        print(team, stats)

    print("\nStep 6: Check SimulationRun in DB...")
    db = SessionLocal()
    runs = db.query(SimulationRun).all()
    print(f"Found {len(runs)} simulation runs.")
    for r in runs[-3:]:
        print(r.id, r.n_runs, r.model_run_id, list(r.results.keys())[:3], "...")
    db.close()