# Load Finder (LLM Quick Reference)

## Purpose
- Single-file FastAPI app that serves an HTML UI and JSON API to scrape (sample data), shortlist, score, and query truck loads.
- Uses SQLite for storage and a deterministic load key for upserts.
- Copyright © 2025 Lighthouse Labs & Innovation.

## Runtime
- Install deps: `pip install -r requirements.txt`
- Run server: `python -m uvicorn load_api:app --reload`
- UI: `http://127.0.0.1:8000/`

## Key Files
- `load_api.py`: FastAPI app, HTML UI, DB logic, scoring logic, endpoints.
- `sample_loads.json`: input dataset used by `/scrape`.
- `loads.db`: default SQLite DB file.

## Environment Defaults
- `LOADS_DB_PATH` (default `loads.db`)
- `SAMPLE_LOADS_PATH` (default `sample_loads.json`)

## Core Flow
1) `/scrape` loads sample JSON into SQLite (insert/update).
2) `/shortlist` tags a subset of loads based on filters.
3) `/loads/score` computes `match_score` for tagged loads.
4) `/loads/query` returns filtered and scored results.

## Endpoints
- `GET /`: HTML UI (single page inside `load_api.py`).
- `GET /health`: `{ "status": "ok" }`.
- `POST /scrape`: `{ overwrite?: bool, db_path?: str, sample_path?: str }`.
  - If `overwrite` is true, deletes all rows from `loads` before inserting.
  - If false, upserts by `load_key` (updates existing rows).
- `POST /shortlist`: filters and tags loads.
  - Payload fields: `tag`, `date`, `O-City`, `O-St`, `D-City`, `D-St`, `O-DH`, `D-DH`, `replace`, `limit`, `only_unscored`.
  - Writes `shortlist_tag` and `shortlisted_at` for matched loads.
- `POST /loads/score`: `{ tag, only_unscored?, limit? }`.
  - Computes `match_score` per load.
- `POST /loads/query`: returns JSON `{ results: [...], count: n }`.
  - Filters by tag/date/locations/deadhead/states.
  - Supports `only_unscored`, `limit`, `offset`.

## Data Model (SQLite: `loads`)
- Primary key: `load_key` (deterministic hash of core fields).
- Common fields: `O-City`, `O-St`, `D-City`, `D-St`, `Pickup`, `Rate`, `Distance`, `Company`, `D2P`.
- Derived fields: `shortlist_tag`, `shortlisted_at`, `match_score`.
- State machine: `NEW`, `READY`, `SCORED`, `APPLIED`, `IGNORED`.
- Upsert preserves `SCORED`, `APPLIED`, `IGNORED` states on refresh.

## Scoring
- `match_score` in `math_match_score()`.
- Inputs: Rate and D2P.
- Weighted blend: `RATE_WEIGHT` and `D2P_WEIGHT`.
- Missing D2P applies penalty.
- Final score clamped to 0..10 and rounded to 1 decimal.

## UI Behavior (HTML in `load_api.py`)
- “Retrieve data” calls `/scrape`.
- “Set filters” triggers shortlist + score + query.
- Auto-refresh triggers the same pipeline on a timer; countdown indicates activity.

## Notes for LLMs
- All logic lives in `load_api.py`; there is no separate frontend build.
- Filtering logic is SQL-based in `run_shortlist()` and `query_loads()`.
- Payloads use aliases like `O-City` and `D-St` (Pydantic aliases set in models).

## License
- Apache-2.0. See `LICENSE` and `NOTICE` for attribution details.
