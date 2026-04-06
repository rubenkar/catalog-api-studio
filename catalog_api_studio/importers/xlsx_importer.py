"""XLSX file importer."""

import logging
from pathlib import Path

import pandas as pd

from catalog_api_studio.importers.base import BaseImporter
from catalog_api_studio.importers.csv_importer import COLUMN_MAP
from catalog_api_studio.models.schemas import ProductCreate

logger = logging.getLogger(__name__)


class XLSXImporter(BaseImporter):
    """Import products from Excel files."""

    def __init__(self, file_path: Path, sheet_name: int | str = 0) -> None:
        super().__init__(file_path)
        self.sheet_name = sheet_name

    def extract(self) -> list[ProductCreate]:
        logger.info("Importing XLSX: %s", self.file_path)

        df = pd.read_excel(self.file_path, sheet_name=self.sheet_name, engine="openpyxl")

        if df.empty:
            logger.warning("XLSX file is empty: %s", self.file_path)
            return []

        col_mapping = self._detect_columns(df.columns.tolist())
        logger.info("Detected column mapping: %s", col_mapping)

        products: list[ProductCreate] = []
        for _, row in df.iterrows():
            product = self._row_to_product(row, col_mapping)
            products.append(product)

        logger.info("Extracted %d products from XLSX", len(products))
        return products

    def _detect_columns(self, headers: list[str]) -> dict[str, str | None]:
        mapping: dict[str, str | None] = {}
        for field, candidates in COLUMN_MAP.items():
            mapping[field] = self.detect_column([str(h) for h in headers], candidates)
        return mapping

    def _row_to_product(
        self, row: pd.Series, col_mapping: dict[str, str | None]
    ) -> ProductCreate:
        def get_val(field: str) -> str | None:
            col = col_mapping.get(field)
            if col and col in row.index:
                val = row[col]
                if pd.notna(val):
                    return str(val).strip()
            return None

        price_str = get_val("price")
        price = None
        if price_str:
            try:
                price = float(price_str.replace(",", ".").replace(" ", ""))
            except ValueError:
                pass

        filled = sum(1 for f in ["sku", "name", "brand", "price"] if get_val(f))
        confidence = filled / 4.0

        return ProductCreate(
            source=str(self.file_path.name),
            sku=get_val("sku"),
            brand=get_val("brand"),
            name=get_val("name"),
            category=get_val("category"),
            description=get_val("description"),
            unit=get_val("unit"),
            price=price,
            stock=get_val("stock"),
            confidence_score=confidence,
        )
