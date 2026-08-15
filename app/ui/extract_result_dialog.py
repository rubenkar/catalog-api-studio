"""Dialog showing bearing extraction results: stats, items table, issues."""

import json
import logging
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QLabel,
    QListWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from app.ui.result_table import result_table_rows

logger = logging.getLogger(__name__)


class ExtractResultDialog(QDialog):
    """View for a <brand>.json extraction result."""

    def __init__(self, json_path: Path, manifest_path: Path | None = None, parent=None) -> None:
        super().__init__(parent)
        result = json.loads(json_path.read_text(encoding="utf-8"))
        manifest = None
        if manifest_path is not None and manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        stats = result.get("stats", {})
        issues = result.get("issues", [])

        self.setWindowTitle(f"Extraction — {result.get('brand', json_path.stem)}")
        self.resize(1000, 700)

        layout = QVBoxLayout(self)

        stats_label = QLabel(
            f"Источник: {result.get('source', '?')}   |   "
            f"Страниц: {stats.get('pages_total', '?')} "
            f"(с данными: {stats.get('pages_with_data', '?')})   |   "
            f"Записей: {stats.get('items_count', '?')}   |   "
            f"Issues: {len(issues)}"
        )
        stats_label.setStyleSheet("font-weight: bold; padding: 4px;")
        layout.addWidget(stats_label)

        headers, rows = result_table_rows(result, manifest)
        table = QTableWidget(len(rows), len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        for r, row in enumerate(rows):
            for c, value in enumerate(row):
                item = QTableWidgetItem(value)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                table.setItem(r, c, item)
        table.setSortingEnabled(True)
        table.resizeColumnsToContents()
        layout.addWidget(table, stretch=1)

        if issues:
            issues_label = QLabel(f"Проблемные места ({len(issues)}):")
            layout.addWidget(issues_label)
            issues_list = QListWidget()
            issues_list.setMaximumHeight(140)
            for issue in issues:
                issues_list.addItem(f"стр. {issue.get('page', '?')}: {issue.get('problem', '')}")
            layout.addWidget(issues_list)
