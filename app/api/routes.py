"""FastAPI route definitions."""

from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db import engine as _db_engine
from app.db.models import Document, ImportJob

router = APIRouter()

_STATIC_DIR = Path(__file__).parent / "static"


def get_db():
    session = _db_engine.get_session()
    try:
        yield session
    finally:
        session.close()


@router.get("/health")
def health(session: Session = Depends(get_db)):
    jobs = session.query(ImportJob).count()
    return {
        "status": "ok",
        "import_jobs": jobs,
    }


@router.get("/", include_in_schema=False)
def dashboard_index():
    return FileResponse(_STATIC_DIR / "dashboard.html")


@router.get("/dashboard", include_in_schema=False)
def dashboard_page():
    return FileResponse(_STATIC_DIR / "dashboard.html")


@router.get("/api/dashboard/stats")
def dashboard_stats(session: Session = Depends(get_db)):
    """Aggregated counters for the dashboard."""
    total_jobs = session.query(ImportJob).count()
    total_documents = session.query(Document).count()
    total_pages = session.query(func.coalesce(func.sum(Document.page_count), 0)).scalar() or 0
    total_products = (
        session.query(func.coalesce(func.sum(ImportJob.products_count), 0)).scalar() or 0
    )

    status_rows = (
        session.query(ImportJob.status, func.count(ImportJob.id))
        .group_by(ImportJob.status)
        .all()
    )
    by_status = {status: count for status, count in status_rows}

    recent_rows = (
        session.query(ImportJob)
        .order_by(ImportJob.created_at.desc())
        .limit(8)
        .all()
    )
    recent = [
        {
            "id": job.id,
            "filename": job.filename,
            "file_type": job.file_type,
            "status": job.status,
            "products_count": job.products_count,
            "created_at": job.created_at.isoformat() if job.created_at else None,
        }
        for job in recent_rows
    ]

    return {
        "totals": {
            "imports": total_jobs,
            "documents": total_documents,
            "pages": int(total_pages),
            "products": int(total_products),
        },
        "by_status": by_status,
        "recent": recent,
    }
