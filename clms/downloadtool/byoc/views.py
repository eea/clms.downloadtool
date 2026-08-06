"""Manager-only view for generating CLMS BYOC metadata."""

import json
import logging
import os
import re
from urllib.parse import quote

from Products.Five.browser import BrowserView
import requests

from clms.types.restapi.mapviewer_service.byoc import BYOC_SNAPSHOT_KEY
from clms.types.restapi.mapviewer_service.byoc import get_byoc_snapshot
from clms.types.restapi.mapviewer_service.byoc import set_byoc_snapshot

from .extractor import BROWSER_FILES
from .extractor import BYOCExtractionError
from .extractor import extract_browser_configuration_from_sources


BROWSER_REPOSITORY = "eu-cdse/copernicus-browser"
DEFAULT_BROWSER_REF = "main"
GITHUB_API = "https://api.github.com/repos"
GITHUB_RAW = "https://raw.githubusercontent.com"
COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$", re.I)
logger = logging.getLogger(__name__)


def _download_browser_sources(reference):
    """Download all extraction inputs from one Browser commit."""
    commit_url = "{api}/{repository}/commits/{reference}".format(
        api=GITHUB_API,
        repository=BROWSER_REPOSITORY,
        reference=quote(reference, safe=""),
    )
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "clms.downloadtool",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        with requests.Session() as session:
            commit_response = session.get(
                commit_url,
                headers=headers,
                timeout=(5, 30),
            )
            commit_response.raise_for_status()
            commit = str(commit_response.json().get("sha") or "")
            if not COMMIT_SHA.fullmatch(commit):
                raise BYOCExtractionError(
                    "GitHub returned an invalid Browser commit"
                )

            sources = {}
            download_report = {
                "status": "success",
                "reference": reference,
                "commit": commit,
                "commitRequestStatus": commit_response.status_code,
                "files": [],
            }
            for source_path in BROWSER_FILES:
                source_url = "{raw}/{repository}/{commit}/{path}".format(
                    raw=GITHUB_RAW,
                    repository=BROWSER_REPOSITORY,
                    commit=commit,
                    path=source_path.as_posix(),
                )
                source_response = session.get(
                    source_url,
                    timeout=(5, 60),
                )
                source_response.raise_for_status()
                content = source_response.content
                sources[source_path] = content.decode("utf-8")
                download_report["files"].append(
                    {
                        "path": source_path.as_posix(),
                        "status": source_response.status_code,
                        "bytes": len(content),
                    }
                )
    except requests.RequestException as error:
        raise BYOCExtractionError(
            "Copernicus Browser sources could not be downloaded: {}".format(
                error
            )
        ) from error
    except (UnicodeDecodeError, ValueError) as error:
        raise BYOCExtractionError(
            "Copernicus Browser sources could not be read: " + str(error)
        ) from error

    return sources, commit, download_report


class UpdateCLMSBYOCView(BrowserView):
    """Extract and return the CLMS BYOC configuration."""

    def __call__(self):
        """Return the validated extraction as formatted JSON."""
        response = self.request.response
        response.setHeader("Content-Type", "application/json")
        reference = os.environ.get(
            "COPERNICUS_BROWSER_REF", DEFAULT_BROWSER_REF
        ).strip() or DEFAULT_BROWSER_REF

        try:
            sources, commit, download_report = _download_browser_sources(
                reference
            )
        except BYOCExtractionError as error:
            logger.error(
                "Copernicus Browser source download failed: %s",
                error,
            )
            response.setStatus(500)
            return json.dumps(
                {
                    "status": "error",
                    "stage": "download",
                    "error": str(error),
                }
            )

        logger.info(
            "Copernicus Browser source download succeeded: "
            "reference=%s commit=%s files=%s",
            reference,
            commit,
            len(download_report["files"]),
        )

        try:
            snapshot = extract_browser_configuration_from_sources(
                sources,
                commit,
            )
            snapshot["source"]["download"] = download_report
            set_byoc_snapshot(snapshot)
            stored_snapshot = get_byoc_snapshot()
            stored_source = stored_snapshot.get("source", {})
            stored_collections = stored_snapshot.get("collections", {})
            commit_matches = stored_source.get("commit") == commit
            collection_count_matches = len(stored_collections) == len(
                snapshot["collections"]
            )
            if not commit_matches or not collection_count_matches:
                raise BYOCExtractionError(
                    "The BYOC snapshot could not be read back from "
                    "portal annotations"
                )
        except BYOCExtractionError as error:
            logger.error(
                "Copernicus Browser BYOC extraction failed: "
                "commit=%s error=%s",
                commit,
                error,
            )
            response.setStatus(500)
            return json.dumps(
                {
                    "status": "error",
                    "stage": "extraction",
                    "download": download_report,
                    "error": str(error),
                }
            )

        collections = snapshot["collections"].values()
        layers = [
            layer
            for collection in collections
            for layer in collection.get("layers", [])
        ]
        logger.info(
            "Copernicus Browser BYOC snapshot stored: "
            "key=%s commit=%s collections=%s layers=%s dual_layers=%s",
            BYOC_SNAPSHOT_KEY,
            commit,
            len(snapshot["collections"]),
            len(layers),
            sum(layer.get("hasLowRes", False) for layer in layers),
        )
        snapshot["storage"] = {
            "status": "success",
            "backend": "portal_annotations",
            "key": BYOC_SNAPSHOT_KEY,
            "verified": True,
        }

        return json.dumps(
            snapshot,
            indent=2,
            sort_keys=True,
        )
