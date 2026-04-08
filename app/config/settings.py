"""Application settings via pydantic-settings."""

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Catalog API Studio"
    debug: bool = False

    # Paths
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    data_dir: Path = base_dir / "data"
    uploads_dir: Path = data_dir / "uploads"
    previews_dir: Path = data_dir / "previews"

    # Database
    database_url: str = f"sqlite:///{base_dir / 'data' / 'catalog.db'}"

    # API
    api_host: str = "127.0.0.1"
    api_port: int = 8000

    # Search
    typesense_host: str = "localhost"
    typesense_port: int = 8108
    typesense_api_key: str = "xyz"
    typesense_collection: str = "products"

    # OCR
    ocr_lang: str = "en"

    # Extraction
    confidence_threshold: float = 0.8

    model_config = {"env_prefix": "CAS_", "env_file": ".env"}


settings = Settings()

# Ensure directories exist
settings.uploads_dir.mkdir(parents=True, exist_ok=True)
settings.previews_dir.mkdir(parents=True, exist_ok=True)
