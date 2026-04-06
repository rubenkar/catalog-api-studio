"""Application entry point — launches PySide6 desktop UI."""

import logging
import sys

from catalog_api_studio.db.engine import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    from PySide6.QtWidgets import QApplication

    from catalog_api_studio.ui.main_window import MainWindow

    logger.info("Starting Catalog API Studio")
    init_db()

    app = QApplication(sys.argv)
    app.setApplicationName("Catalog API Studio")
    app.setOrganizationName("CatalogAPIStudio")

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
