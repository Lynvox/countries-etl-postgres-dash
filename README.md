# Countries ETL + Dash

Junior Data Engineer test task implementation:
- Download countries data from [REST Countries](https://restcountries.com/)
- Transform into a pandas DataFrame
- Store in PostgreSQL (Docker Compose)
- Visualize in Dash (sortable table + selected country flag)

## Project Files

- `download_countries.py` - fetch and normalize API data into DataFrame
- `load_to_db.py` - load DataFrame into PostgreSQL table `countries`
- `app.py` - Dash UI (table + flag preview)
- `db_config.py` - PostgreSQL settings via env variables
- `docker-compose.yml` - PostgreSQL container

## Requirements

- Python 3.10+
- Docker + Docker Compose

## Quick Start

```bash
docker compose up -d

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python load_to_db.py
python app.py
```

Open: `http://127.0.0.1:8050` (or `http://<server-ip>:8050`).

## Environment Variables (optional)

Defaults are:
- `PGHOST=localhost`
- `PGPORT=5433`
- `PGUSER=countries`
- `PGPASSWORD=countries`
- `PGDATABASE=countries`

If needed:

```bash
export PGPORT=5433
python load_to_db.py
python app.py
```

## Stop Database

```bash
docker compose down
```
