from pathlib import Path

import pandas as pd

from services.s3_client import s3_client

if __name__ == "__main__":
    print("Testing local S3 client (mock_s3)...")

    # 1. Write a small CSV via write_csv
    df = pd.DataFrame(
        {
            "team": ["A", "B", "C"],
            "goals": [1, 2, 3],
        }
    )
    key = "test/test_table.csv"
    s3_client.write_csv(df, key)
    print(f"Written DataFrame to {key}")

    # 2. Read it back
    df2 = s3_client.read_csv(key)
    print("Read back from mock S3:")
    print(df2)

    # 3. Upload a file with upload_file/download_file
    tmp_dir = Path("tmp_test_files")
    tmp_dir.mkdir(exist_ok=True)

    local_path = tmp_dir / "local_file.txt"
    local_path.write_text("hello from local file")

    key2 = "test/local_file_uploaded.txt"
    s3_client.upload_file(local_path, key2)
    print(f"Uploaded {local_path} to {key2}")

    download_path = tmp_dir / "local_file_downloaded.txt"
    s3_client.download_file(key2, download_path)
    print(f"Downloaded {key2} to {download_path}")

    print("Downloaded file content:", download_path.read_text())