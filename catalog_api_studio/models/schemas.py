"""Pydantic schemas for API and internal data transfer."""

from datetime import datetime

from pydantic import BaseModel, Field


# --- Product ---


class ProductBase(BaseModel):
    source: str | None = None
    sku: str | None = None
    brand: str | None = None
    name: str | None = None
    category: str | None = None
    description: str | None = None
    attributes: dict | None = None
    unit: str | None = None
    price: float | None = None
    stock: str | None = None
    documents: list | None = None
    images: list | None = None


class ProductCreate(ProductBase):
    import_job_id: int | None = None
    confidence_score: float = 0.0


class ProductUpdate(BaseModel):
    source: str | None = None
    sku: str | None = None
    brand: str | None = None
    name: str | None = None
    category: str | None = None
    description: str | None = None
    attributes: dict | None = None
    unit: str | None = None
    price: float | None = None
    stock: str | None = None
    documents: list | None = None
    images: list | None = None
    reviewed: bool | None = None


class ProductResponse(ProductBase):
    id: int
    import_job_id: int | None = None
    confidence_score: float
    reviewed: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ProductList(BaseModel):
    items: list[ProductResponse]
    total: int
    page: int
    per_page: int


# --- Import Job ---


class ImportJobCreate(BaseModel):
    filename: str
    file_type: str


class ImportJobResponse(BaseModel):
    id: int
    filename: str
    file_type: str
    status: str
    error_message: str | None = None
    products_count: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# --- Search ---


class SearchQuery(BaseModel):
    q: str = ""
    brand: str | None = None
    category: str | None = None
    page: int = Field(default=1, ge=1)
    per_page: int = Field(default=20, ge=1, le=100)


class SearchResult(BaseModel):
    items: list[ProductResponse]
    total: int
    query: str


# --- Filters ---


class FilterResponse(BaseModel):
    brands: list[str]
    categories: list[str]
    units: list[str]
