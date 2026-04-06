"""SQLAlchemy ORM models."""

from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class ImportJob(Base):
    __tablename__ = "import_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    file_type: Mapped[str] = mapped_column(String(10), nullable=False)  # pdf, xlsx, csv
    file_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )  # pending, processing, done, error
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    products_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    documents: Mapped[list["Document"]] = relationship(back_populates="import_job")
    products: Mapped[list["Product"]] = relationship(back_populates="import_job")


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    import_job_id: Mapped[int] = mapped_column(ForeignKey("import_jobs.id"), nullable=False)
    file_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    page_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    processing_mode: Mapped[str | None] = mapped_column(
        String(20), nullable=True
    )  # native, bitmap, hybrid

    import_job: Mapped["ImportJob"] = relationship(back_populates="documents")
    page_previews: Mapped[list["PagePreview"]] = relationship(back_populates="document")


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    import_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_jobs.id"), nullable=True
    )
    source: Mapped[str | None] = mapped_column(String(500), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(200), nullable=True, index=True)
    brand: Mapped[str | None] = mapped_column(String(200), nullable=True, index=True)
    name: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    category: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    attributes: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    stock: Mapped[str | None] = mapped_column(String(100), nullable=True)
    documents: Mapped[list | None] = mapped_column(JSON, nullable=True)
    images: Mapped[list | None] = mapped_column(JSON, nullable=True)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    reviewed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    import_job: Mapped["ImportJob | None"] = relationship(back_populates="products")


class PagePreview(Base):
    __tablename__ = "page_previews"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("documents.id"), nullable=False)
    page_num: Mapped[int] = mapped_column(Integer, nullable=False)
    image_path: Mapped[str] = mapped_column(String(1000), nullable=False)

    document: Mapped["Document"] = relationship(back_populates="page_previews")
