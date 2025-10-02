# Assignment-4 (Relational & Document DBs)

## this repo contains
- `src/` — all Python scripts for Q1..Q4:
  - `q1.py` — create relational DB (SQLite) and insert 1000 rows
  - `q2.py` — load SQLite → MongoDB (transaction & customer models)
  - `q2_verify.py` — For verify that database successfully work  
  - `q3.py` — CRUD performance comparison and timings
  - `q4.py` — (safe) load to MongoDB; reads MONGO_URI from env
  - `q4_verify.py` — verify connection & counts on MongoDB
- `schema.sql` — DDL for the relational schema
- `online_retail.db` — SQLite DB used by scripts (small, included)
- `pyproject.toml`, `uv.lock` — uv-managed dependency files

## How to run 
  -- uv sync 
  -- uv run python src/q1.py
