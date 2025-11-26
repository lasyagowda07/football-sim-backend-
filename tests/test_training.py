from services.data_ingestion import run_ingestion
from services.data_processing import run_processing
from services.train_model import run_training
from services.model_registry import list_model_runs, load_active_model

if __name__ == "__main__":
    print("Step 1: Ingestion...")
    print(run_ingestion())

    print("\nStep 2: Processing...")
    print(run_processing())

    print("\nStep 3: Training...")
    train_result = run_training()
    print("Training result:")
    print(train_result)

    print("\nStep 4: List model runs from DB...")
    runs = list_model_runs()
    print(f"Found {len(runs)} model runs:")
    for r in runs:
        print(r.id, r.status, r.model_s3_path, r.metrics)

    print("\nStep 5: Load active model via registry...")
    artifact = load_active_model()
    print("Loaded artifact type:", type(artifact))
    print("Artifact keys:", list(artifact.keys()))
    print("Model type:", type(artifact["model"]))
    print("Label encoder type:", type(artifact["label_encoder"]))