"""OCR engine wrapper — PaddleOCR with lazy initialization."""

import logging

from PIL import Image

logger = logging.getLogger(__name__)


class OCREngine:
    """Wrapper around PaddleOCR with lazy initialization."""

    _instance: "OCREngine | None" = None
    _ocr = None

    def __new__(cls) -> "OCREngine":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _init_ocr(self) -> None:
        if self._ocr is not None:
            return

        logger.info("Initializing PaddleOCR (this may take a moment)...")
        try:
            from paddleocr import PaddleOCR

            from catalog_api_studio.config.settings import settings

            self._ocr = PaddleOCR(
                use_angle_cls=True,
                lang=settings.ocr_lang,
                show_log=False,
            )
            logger.info("PaddleOCR initialized successfully")
        except ImportError:
            logger.error("PaddleOCR not installed. Install with: pip install paddleocr paddlepaddle")
            raise

    def extract_text(self, image: Image.Image) -> list[dict]:
        """Extract text blocks from a PIL Image.

        Returns list of dicts with keys: text, bbox, confidence
        """
        self._init_ocr()

        import numpy as np

        img_array = np.array(image)
        result = self._ocr.ocr(img_array, cls=True)

        text_blocks: list[dict] = []
        if not result or not result[0]:
            return text_blocks

        for line in result[0]:
            bbox = line[0]  # [[x1,y1], [x2,y2], [x3,y3], [x4,y4]]
            text = line[1][0]
            confidence = line[1][1]

            x_min = min(p[0] for p in bbox)
            y_min = min(p[1] for p in bbox)
            x_max = max(p[0] for p in bbox)
            y_max = max(p[1] for p in bbox)

            text_blocks.append({
                "text": text,
                "bbox": [x_min, y_min, x_max, y_max],
                "confidence": confidence,
            })

        return text_blocks
