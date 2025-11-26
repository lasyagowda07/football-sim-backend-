from __future__ import annotations

from functools import lru_cache
from typing import List

import pandas as pd

from services.s3_client import s3_client


@lru_cache(maxsize=1)
def get_teams() -> List[str]:
    """
    Load processed/matches.csv from S3 (mock_s3 locally) and
    return a sorted list of unique team names.

    Cached in-memory so we don't keep hitting disk.
    """
    df: pd.DataFrame = s3_client.read_csv("processed/matches.csv")

    home_teams = df["home_team"].dropna().astype(str)
    away_teams = df["away_team"].dropna().astype(str)

    unique_teams = sorted(set(home_teams) | set(away_teams))
    return unique_teams