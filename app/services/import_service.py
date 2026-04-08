"""Import service — orchestrates file import and DB storage."""

import logging
import shutil
from pathlib import Path

from app.config.settings import settings
from app.db.engine import get_session
from app.db.models import Document, ImportJob

logger = logging.getLogger(__name__)


class ImportService:
    """Orchestrates the file import pipeline."""

    def import_file(self, file_path: Path) -> ImportJob:
        """Import a file: copy to uploads, create job record."""
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

            job.status = "done"
            session.commit()
            logger.info("Import job #%d completed", job.id)
            return job

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
