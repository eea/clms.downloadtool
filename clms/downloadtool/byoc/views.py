"""Manager-only view for generating CLMS BYOC metadata."""

import json
import os
import re
from urllib.parse import quote

from Products.Five.browser import BrowserView
import requests

from .extractor import BROWSER_FILES
from .extractor import BYOCExtractionError
from .extractor import extract_browser_configuration_from_sources


BROWSER_REPOSITORY = "eu-cdse/copernicus-browser"
DEFAULT_BROWSER_REF = "main"
GITHUB_API = "https://api.github.com/repos"
GITHUB_RAW = "https://raw.githubusercontent.com"
COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$", re.I)


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
            "Copernicus Browser sources could not be downloaded: "
            + str(error)
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
        reference = (
            os.environ.get("COPERNICUS_BROWSER_REF", DEFAULT_BROWSER_REF)
            .strip()
            or DEFAULT_BROWSER_REF
        )

        try:
            sources, commit, download_report = _download_browser_sources(
                reference
            )
        except BYOCExtractionError as error:
            response.setStatus(500)
            return json.dumps(
                {
                    "status": "error",
                    "stage": "download",
                    "error": str(error),
                }
            )

        try:
            snapshot = extract_browser_configuration_from_sources(
                sources,
                commit,
            )
            snapshot["source"]["download"] = download_report
        except BYOCExtractionError as error:
            response.setStatus(500)
            return json.dumps(
                {
                    "status": "error",
                    "stage": "extraction",
                    "download": download_report,
                    "error": str(error),
                }
            )

        return json.dumps(
            snapshot,
            indent=2,
            sort_keys=True,
        )
