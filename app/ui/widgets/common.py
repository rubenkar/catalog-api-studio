"""Shared UI widgets."""

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel


class StatusLabel(QLabel):
    """Label that changes color based on status."""

    STATUS_COLORS = {
        "pending": "#888",
        "processing": "#f0ad4e",
        "done": "#5cb85c",
        "error": "#d9534f",
        "ok": "#5cb85c",
        "stopped": "#888",
    }

    def set_status(self, status: str) -> None:
        color = self.STATUS_COLORS.get(status, "#333")
        self.setText(status.upper())
        self.setStyleSheet(f"color: {color}; font-weight: bold;")


class ConfidenceBadge(QLabel):
    """Colored badge showing confidence score."""

    def set_confidence(self, score: float) -> None:
        if score < 0.5:
            color = "#d9534f"  # red
        elif score < 0.8:
            color = "#f0ad4e"  # yellow
        else:
            color = "#5cb85c"  # green

        self.setText(f"{score:.0%}")
        self.setStyleSheet(
            f"background-color: {color}; color: white; padding: 2px 6px; "
            f"border-radius: 3px; font-weight: bold;"
        )
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
