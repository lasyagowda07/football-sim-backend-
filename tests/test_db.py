from core.db import SessionLocal
from models.model_run import ModelRun

if __name__ == "__main__":
    db = SessionLocal()

    # Just query all model runs (should be empty if you haven't trained yet)
    runs = db.query(ModelRun).all()
    print(f"Found {len(runs)} model runs.")

    for r in runs:
        print(r.id, r.status, r.model_s3_path, r.metrics)

    db.close()