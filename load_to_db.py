"""
Load DataFrame from download_countries.py into PostgreSQL (countries table).
"""

from sqlalchemy import create_engine

from db_config import sqlalchemy_url
from download_countries import fetch_countries_df


def load() -> int:
    df = fetch_countries_df()
    engine = create_engine(sqlalchemy_url(), future=True)
    # Replace table on each run to keep a clean current snapshot from API.
    df.to_sql(
        "countries",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=100,
    )
    return len(df)


if __name__ == "__main__":
    n = load()
    print(f"Loaded {n} rows into table 'countries'.")

