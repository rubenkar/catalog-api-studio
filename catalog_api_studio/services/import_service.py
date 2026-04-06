"""Import service — orchestrates file import, extraction, and DB storage."""

import logging
import shutil
from pathlib import Path

from catalog_api_studio.config.settings import settings
from catalog_api_studio.db.engine import get_session
from catalog_api_studio.db.models import Document, ImportJob, Product
from catalog_api_studio.models.schemas import ProductCreate
from catalog_api_studio.normalization.normalizer import Normalizer

logger = logging.getLogger(__name__)


class ImportService:
    """Orchestrates the full import pipeline."""

    def __init__(self) -> None:
        self.normalizer = Normalizer()

    def import_file(self, file_path: Path) -> ImportJob:
        """Import a file: copy to uploads, create job, extract products."""
        session = get_session()
        try:
            suffix = file_path.suffix.lower().lstrip(".")
            if suffix not in ("pdf", "xlsx", "csv"):
                raise ValueError(f"Unsupported file type: {suffix}")

            # Copy file to uploads
            dest = settings.uploads_dir / file_path.name
            if dest.exists():
                stem = file_path.stem
                dest = settings.uploads_dir / f"{stem}_{id(self)}{file_path.suffix}"
            shutil.copy2(file_path, dest)

            # Create import job
            job = ImportJob(
                filename=file_path.name,
                file_type=suffix,
                file_path=str(dest),
                status="processing",
            )
            session.add(job)
            session.commit()

            logger.info("Created import job #%d for %s", job.id, file_path.name)

            # Create document record
            doc = Document(
                import_job_id=job.id,
                file_path=str(dest),
            )
            session.add(doc)
            session.commit()

            # Extract products
            try:
                products = self._extract(dest, suffix)
                self._save_products(session, job, products)
                job.status = "done"
                job.products_count = len(products)
                logger.info("Import job #%d completed: %d products", job.id, len(products))
            except Exception as e:
                job.status = "error"
                job.error_message = str(e)
                logger.error("Import job #%d failed: %s", job.id, e)

            session.commit()
            return job

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _extract(self, file_path: Path, file_type: str) -> list[ProductCreate]:
        if file_type == "csv":
            from catalog_api_studio.importers.csv_importer import CSVImporter

            return CSVImporter(file_path).extract()
        elif file_type == "xlsx":
            from catalog_api_studio.importers.xlsx_importer import XLSXImporter

            return XLSXImporter(file_path).extract()
        elif file_type == "pdf":
            from catalog_api_studio.importers.pdf_importer import PDFImporter

            return PDFImporter(file_path).extract()
        else:
            raise ValueError(f"Unsupported: {file_type}")

    def _save_products(
        self, session, job: ImportJob, products: list[ProductCreate]
    ) -> None:
        for p in products:
            normalized = self.normalizer.normalize_product(p.model_dump())
            product = Product(
                import_job_id=job.id,
                **{k: v for k, v in normalized.items() if k != "import_job_id"},
            )
            session.add(product)
        session.commit()
