"""FastAPI route definitions."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import engine as _db_engine
from app.db.models import ImportJob

router = APIRouter()


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
