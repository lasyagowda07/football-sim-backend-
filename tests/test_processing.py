from services.data_ingestion import run_ingestion
from services.data_processing import run_processing
from services.s3_client import s3_client

if __name__ == "__main__":
    print("Step 1: Ingestion...")
    ingest_result = run_ingestion()
    print("Ingestion result:", ingest_result)

    print("\nStep 2: Processing...")
    proc_result = run_processing()
    print("Processing result:", proc_result)

    # Quick sanity check: read processed files back
    print("\nReading processed/matches.csv from mock_s3...")
    matches = s3_client.read_csv("processed/matches.csv")
    print("matches.shape =", matches.shape)
    print(matches.head())

    print("\nReading processed/teams.csv from mock_s3...")
    teams = s3_client.read_csv("processed/teams.csv")
    print("teams.shape =", teams.shape)
    print(teams.head())