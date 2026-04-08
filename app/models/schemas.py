"""Pydantic schemas for API and internal data transfer."""

from datetime import datetime

from pydantic import BaseModel, Field


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
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# --- Search ---


class SearchQuery(BaseModel):
    q: str = ""
    page: int = Field(default=1, ge=1)
    per_page: int = Field(default=20, ge=1, le=100)
