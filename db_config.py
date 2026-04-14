"""
PostgreSQL connection settings without a single DATABASE_URL value.
Defaults match docker-compose.yml; on a server they can be overridden
via PGHOST, PGPORT, PGUSER, PGPASSWORD, and PGDATABASE environment variables.
"""

import os
from urllib.parse import quote_plus

PG_HOST = os.environ.get("PGHOST", "localhost")
PG_PORT = int(os.environ.get("PGPORT", "5433"))
PG_USER = os.environ.get("PGUSER", "countries")
PG_PASSWORD = os.environ.get("PGPASSWORD", "countries")
PG_DATABASE = os.environ.get("PGDATABASE", "countries")


def sqlalchemy_url() -> str:
    """URL used only by SQLAlchemy engine (not stored as DATABASE_URL)."""
    u = quote_plus(PG_USER)
    p = quote_plus(PG_PASSWORD)
    return f"postgresql+psycopg2://{u}:{p}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
