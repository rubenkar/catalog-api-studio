"""Application entry point — launches PySide6 desktop UI."""

import logging
import sys

from app.db.engine import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    try:
        from PySide6.QtWidgets import QApplication
    except ModuleNotFoundError as exc:
        if exc.name != "PySide6":
            raise
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        logger.error(
            "PySide6 is not installed in the active Python environment "
            "(current interpreter: Python %s).",
            python_version,
        )
        logger.error(
            "Create and activate a Python 3.11 virtual environment, then install deps with:\n"
            "  py -3.11 -m venv .venv\n"
            "  .\\.venv\\Scripts\\Activate.ps1\n"
            "  python -m pip install -e \".[dev]\""
        )
        raise SystemExit(1) from exc

    from app.ui.main_window import MainWindow

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
