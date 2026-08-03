"""Manager-only view for generating CLMS BYOC metadata."""

import json
import os
from pathlib import Path

from Products.Five.browser import BrowserView

from .extractor import BYOCExtractionError
from .extractor import extract_browser_configuration


class UpdateCLMSBYOCView(BrowserView):
    """Extract and return the CLMS BYOC configuration."""

    def __call__(self):
        """Return the validated extraction as formatted JSON."""
        response = self.request.response
        response.setHeader("Content-Type", "application/json")
        browser_path = os.environ.get("COPERNICUS_BROWSER_PATH") or (
            Path(__file__).resolve().parents[4] / "copernicus-browser"
        )

        try:
            snapshot = extract_browser_configuration(browser_path)
        except BYOCExtractionError as error:
            response.setStatus(500)
            return json.dumps({"status": "error", "error": str(error)})

        return json.dumps(
            snapshot,
            indent=2,
            sort_keys=True,
        )
