# Catalog API Studio — Project Conventions

## Stack
- Python 3.11+, PySide6, FastAPI, SQLAlchemy 2.0, Pydantic v2
- SQLite for MVP (PostgreSQL-ready architecture)
- Typesense for search (SQLite LIKE fallback)
- PaddleOCR for bitmap PDF extraction

## Structure
- `catalog_api_studio/` — main application package
- `app/` — legacy scrapers and Flask debug UI (do not modify unless needed)
- `tests/` — pytest test suite
- `data/` — scraped data, uploads, catalogs

## Conventions
- Type hints everywhere
- Pydantic models for all data schemas
- SQLAlchemy ORM for database models
- Logging via `logging` stdlib (no print)
- Background tasks via QThread (UI) or asyncio (API)

## Commands
- Install: `pip install -e ".[dev]"`
- Run app: `python -m catalog_api_studio.main`
- Run tests: `pytest`
- Lint: `ruff check .`
