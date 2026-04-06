"""API tab — FastAPI server control and status."""

import logging
import threading

import uvicorn
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from catalog_api_studio.config.settings import settings

logger = logging.getLogger(__name__)


class APIView(QWidget):
    """API server control panel."""

    def __init__(self) -> None:
        super().__init__()
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Server controls
        controls = QHBoxLayout()

        self.start_btn = QPushButton("Start API Server")
        self.start_btn.setMinimumHeight(40)
        self.start_btn.clicked.connect(self._toggle_server)
        controls.addWidget(self.start_btn)

        self.status_label = QLabel("Stopped")
        self.status_label.setStyleSheet("color: #888; font-weight: bold; font-size: 14px;")
        controls.addWidget(self.status_label)

        controls.addStretch()
        layout.addLayout(controls)

        # Server info
        info_layout = QVBoxLayout()

        self.url_label = QLabel(
            f"API URL: http://{settings.api_host}:{settings.api_port}"
        )
        self.url_label.setStyleSheet("font-size: 13px;")
        info_layout.addWidget(self.url_label)

        self.docs_label = QLabel(
            f"OpenAPI Docs: http://{settings.api_host}:{settings.api_port}/docs"
        )
        self.docs_label.setStyleSheet("font-size: 13px; color: #337ab7;")
        info_layout.addWidget(self.docs_label)

        layout.addLayout(info_layout)

        # Endpoints list
        layout.addWidget(QLabel(""))
        endpoints_title = QLabel("Available Endpoints:")
        endpoints_title.setStyleSheet("font-weight: bold; font-size: 13px;")
        layout.addWidget(endpoints_title)

        endpoints = [
            "GET /health — Server health and product count",
            "GET /products — Product list (paginated)",
            "GET /products/{id} — Product detail",
            "GET /search?q= — Full-text search",
            "GET /brands — All brands",
            "GET /categories — All categories",
            "GET /filters — Available filter values",
        ]
        for ep in endpoints:
            label = QLabel(f"  {ep}")
            label.setStyleSheet("font-family: monospace; font-size: 12px;")
            layout.addWidget(label)

        layout.addStretch()

    def _toggle_server(self) -> None:
        if self._running:
            self._stop_server()
        else:
            self._start_server()

    def _start_server(self) -> None:
        from catalog_api_studio.api.app import create_app

        app = create_app()

        config = uvicorn.Config(
            app,
            host=settings.api_host,
            port=settings.api_port,
            log_level="info",
        )
        self._server = uvicorn.Server(config)

        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()

        self._running = True
        self.start_btn.setText("Stop API Server")
        self.status_label.setText("Running")
        self.status_label.setStyleSheet("color: #5cb85c; font-weight: bold; font-size: 14px;")

        logger.info("API server started on %s:%s", settings.api_host, settings.api_port)

    def _stop_server(self) -> None:
        if self._server:
            self._server.should_exit = True
            self._thread = None
            self._server = None

        self._running = False
        self.start_btn.setText("Start API Server")
        self.status_label.setText("Stopped")
        self.status_label.setStyleSheet("color: #888; font-weight: bold; font-size: 14px;")

        logger.info("API server stopped")

    def refresh(self) -> None:
        pass
