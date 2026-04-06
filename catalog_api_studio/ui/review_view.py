"""Review tab — low-confidence products with inline editing."""

import logging

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from catalog_api_studio.db.engine import get_session
from catalog_api_studio.models.schemas import ProductUpdate
from catalog_api_studio.services.product_service import ProductService

logger = logging.getLogger(__name__)


class ProductEditDialog(QDialog):
    """Dialog for editing a single product."""

    def __init__(self, product, parent=None) -> None:
        super().__init__(parent)
        self.product = product
        self.setWindowTitle(f"Edit Product #{product.id}")
        self.setMinimumWidth(500)

        layout = QFormLayout(self)

        self.fields: dict[str, QLineEdit | QTextEdit] = {}

        for field_name, label, value in [
            ("sku", "SKU", product.sku),
            ("brand", "Brand", product.brand),
            ("name", "Name", product.name),
            ("category", "Category", product.category),
            ("unit", "Unit", product.unit),
            ("price", "Price", str(product.price) if product.price else ""),
            ("stock", "Stock", product.stock),
        ]:
            edit = QLineEdit(value or "")
            self.fields[field_name] = edit
            layout.addRow(label, edit)

        # Description as text area
        desc_edit = QTextEdit(product.description or "")
        desc_edit.setMaximumHeight(100)
        self.fields["description"] = desc_edit
        layout.addRow("Description", desc_edit)

        # Confidence display
        conf_label = QLabel(f"{product.confidence_score:.0%}")
        layout.addRow("Confidence", conf_label)

        # Buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def get_update_data(self) -> ProductUpdate:
        price_str = self.fields["price"].text().strip()
        price = None
        if price_str:
            try:
                price = float(price_str.replace(",", "."))
            except ValueError:
                pass

        desc = self.fields["description"]
        description = desc.toPlainText().strip() if isinstance(desc, QTextEdit) else desc.text().strip()

        return ProductUpdate(
            sku=self.fields["sku"].text().strip() or None,
            brand=self.fields["brand"].text().strip() or None,
            name=self.fields["name"].text().strip() or None,
            category=self.fields["category"].text().strip() or None,
            unit=self.fields["unit"].text().strip() or None,
            price=price,
            stock=self.fields["stock"].text().strip() or None,
            description=description or None,
            reviewed=True,
        )


class ReviewView(QWidget):
    """Review tab — shows low-confidence products for manual correction."""

    def __init__(self) -> None:
        super().__init__()
        self._products: list = []
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Top bar
        top_bar = QHBoxLayout()

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh)
        top_bar.addWidget(self.refresh_btn)

        self.count_label = QLabel("0 items to review")
        top_bar.addWidget(self.count_label)

        top_bar.addStretch()

        info = QLabel("Double-click a row to edit")
        info.setStyleSheet("color: #666;")
        top_bar.addWidget(info)

        layout.addLayout(top_bar)

        # Table
        columns = ["ID", "SKU", "Brand", "Name", "Category", "Price", "Confidence", "Reviewed"]
        self.table = QTableWidget()
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(columns)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.doubleClicked.connect(self._on_double_click)
        layout.addWidget(self.table)

    def refresh(self) -> None:
        session = get_session()
        try:
            svc = ProductService(session)
            self._products = svc.get_review_products()
            self.count_label.setText(f"{len(self._products)} items to review")
            self.table.setRowCount(len(self._products))

            for row, p in enumerate(self._products):
                self.table.setItem(row, 0, QTableWidgetItem(str(p.id)))
                self.table.setItem(row, 1, QTableWidgetItem(p.sku or ""))
                self.table.setItem(row, 2, QTableWidgetItem(p.brand or ""))
                self.table.setItem(row, 3, QTableWidgetItem(p.name or ""))
                self.table.setItem(row, 4, QTableWidgetItem(p.category or ""))
                self.table.setItem(row, 5, QTableWidgetItem(
                    f"{p.price:.2f}" if p.price else ""
                ))

                # Confidence with color
                conf_item = QTableWidgetItem(f"{p.confidence_score:.0%}")
                if p.confidence_score < 0.5:
                    conf_item.setForeground(QColor("#d9534f"))
                elif p.confidence_score < 0.8:
                    conf_item.setForeground(QColor("#f0ad4e"))
                else:
                    conf_item.setForeground(QColor("#5cb85c"))
                conf_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row, 6, conf_item)

                reviewed_item = QTableWidgetItem("Yes" if p.reviewed else "No")
                self.table.setItem(row, 7, reviewed_item)
        finally:
            session.close()

    def _on_double_click(self, index) -> None:
        row = index.row()
        if row >= len(self._products):
            return

        product = self._products[row]
        dialog = ProductEditDialog(product, self)

        if dialog.exec() == QDialog.DialogCode.Accepted:
            update_data = dialog.get_update_data()
            session = get_session()
            try:
                svc = ProductService(session)
                svc.update(product.id, update_data)
                QMessageBox.information(self, "Saved", f"Product #{product.id} updated")
                self.refresh()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))
            finally:
                session.close()
