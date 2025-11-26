from core.config import settings

if __name__ == "__main__":
    print("ENV:", settings.ENV)
    print("DB_URL:", settings.DB_URL)
    print("S3_BUCKET:", settings.S3_BUCKET)
    print("MOCK_S3_ROOT:", settings.MOCK_S3_ROOT)