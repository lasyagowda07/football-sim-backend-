from core.db import Base, engine
from models.model_run import ModelRun  # noqa: F401 (import so table is registered)


def init_db():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Done. Tables created.")


if __name__ == "__main__":
    init_db()