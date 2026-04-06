"""SQLAlchemy engine and session factory."""

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from catalog_api_studio.config.settings import settings

engine = create_engine(
    settings.database_url,
    echo=settings.debug,
    connect_args={"check_same_thread": False},  # SQLite
)

SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


def get_session() -> Session:
    """Get a new database session."""
    return SessionLocal()


def init_db() -> None:
    """Create all tables."""
    from catalog_api_studio.db.models import Base

    Base.metadata.create_all(bind=engine)
