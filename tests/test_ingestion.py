from services.data_ingestion import run_ingestion

if __name__ == "__main__":
    print("Running data ingestion from ./data to mock_s3/raw...")
    result = run_ingestion()
    print("Ingestion result:")
    print(result)