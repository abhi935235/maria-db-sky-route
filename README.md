## MariaDB OpenFlights Hackathon Project

This reference implementation demonstrates MariaDB features with the OpenFlights dataset:

- ColumnStore for analytics at scale
- Optional Galera Cluster for HA
- Python FastAPI app for loading data and exposing simple analytics endpoints

### Prerequisites

- Docker Desktop
- Python 3.11+

### Quick Start (single-node MariaDB)

1. Copy `.env.example` to `.env` and adjust if needed.
2. Start MariaDB:
   ```bash
   docker compose up -d mariadb
   ```
3. Create and activate a virtual environment, then install deps:
   ```bash
   python -m venv .venv
   . .venv/Scripts/Activate.ps1  # PowerShell on Windows
   pip install -r requirements.txt
   ```
4. Download OpenFlights CSVs:
   ```bash
   ./scripts/download-openflights.ps1
   ```
5. Load data and run API:
   ```bash
   uvicorn src.main:app --reload
   ```

### ColumnStore Profile

- Bring up ColumnStore server:
  ```bash
  docker compose --profile columnstore up -d columnstore
  ```
- Use `COLUMNSTORE_HOST`/`COLUMNSTORE_PORT` from `.env` for analytics queries targeting ColumnStore.

### Galera Profile (Optional)

Demonstrates HA with 3-node cluster. Bootstrap node1 and then others:

```bash
docker compose --profile galera up -d galera-node1
docker compose --profile galera up -d galera-node2 galera-node3
```

Point your app at any node's port (3308/3309/3310) to test.

### Endpoints

- `GET /health`
- `POST /load` — load OpenFlights CSVs into MariaDB
- `GET /analytics/top-airports?limit=10`
- `GET /analytics/routes-between?src=JFK&dst=LHR`
- `GET /analytics/nearest-airports?lat=40.6413&lon=-73.7781&k=5` (uses MariaDB Vector via Haversine fallback; swap-in vector index if available)

### Dataset

We use `https://github.com/MariaDB/openflights`. The PowerShell script pulls the CSVs into `data/`.


