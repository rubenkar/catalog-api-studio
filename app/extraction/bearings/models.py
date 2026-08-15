"""Pydantic schemas for the bearing catalog extraction pipeline."""

from pydantic import BaseModel, Field


class FieldSpec(BaseModel):
    key: str
    label: str
    unit: str | None = None
    core: bool = False


class Manifest(BaseModel):
    source: str
    brand: str
    fields: list[FieldSpec]

    def field_keys(self) -> list[str]:
        return [f.key for f in self.fields]


class ColumnSpec(BaseModel):
    x_min: float
    x_max: float
    field: str


class ParseRule(BaseModel):
    header_skip_lines: int = 0
    columns: list[ColumnSpec]
    row_gap_pt: float = 4.0
    inherit_fields: list[str] = Field(default_factory=list)
    type_default: str | None = None


class Issue(BaseModel):
    page: int
    problem: str


class CatalogResult(BaseModel):
    source: str
    brand: str
    extracted_at: str
    pipeline_version: str = "1.0"
    items: list[dict]
    stats: dict
    issues: list[Issue]
