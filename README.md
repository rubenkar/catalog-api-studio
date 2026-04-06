# Catalog API Studio

Local Python desktop application for digitizing supplier catalogs into structured product data, REST API, and smart search.

## Features

- **File Import**: PDF, XLSX, CSV catalog upload
- **PDF Pipeline**: Native text extraction, bitmap OCR fallback, hybrid merge
- **OCR**: PaddleOCR for scanned/bitmap PDFs
- **Extraction**: Automatic product data extraction with confidence scoring
- **Review**: Human-in-the-loop correction of uncertain records
- **Search**: Full-text search via Typesense (SQLite fallback)
- **REST API**: FastAPI with OpenAPI docs
- **Export**: JSON and CSV export

## Quick Start

### Prerequisites

- Python 3.11+
- (Optional) Typesense for full-text search

### Install

```bash
git clone https://github.com/rubenkar/catalog-api-studio.git
cd catalog-api-studio
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

### Run

```bash
python -m catalog_api_studio.main
```

### Run Tests

```bash
pytest
```

### API Only

```bash
uvicorn catalog_api_studio.api.app:create_app --factory --host 127.0.0.1 --port 8000
```

## Architecture

```
catalog_api_studio/
├── api/          # FastAPI REST endpoints
├── config/       # Pydantic settings
├── db/           # SQLAlchemy models + engine
├── extraction/   # Raw data → product records
├── importers/    # CSV, XLSX, PDF file importers
├── models/       # Pydantic schemas
├── normalization/# SKU, unit, brand normalization
├── ocr/          # PaddleOCR wrapper
├── pdf/          # Native, bitmap, hybrid extraction
├── search/       # Typesense indexer + SQLite fallback
├── services/     # Import, product, export services
└── ui/           # PySide6 desktop interface
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Server status |
| `GET /products` | Product list (paginated) |
| `GET /products/{id}` | Product detail |
| `GET /search?q=` | Full-text search |
| `GET /brands` | All brands |
| `GET /categories` | All categories |
| `GET /filters` | Filter options |

## Stack

- **UI**: PySide6
- **API**: FastAPI + Uvicorn
- **DB**: SQLite (PostgreSQL-ready via SQLAlchemy)
- **Search**: Typesense (SQLite LIKE fallback)
- **OCR**: PaddleOCR
- **PDF**: pdfplumber + PyMuPDF
