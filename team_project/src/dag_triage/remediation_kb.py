"""Remediation knowledge base for Airflow failure categories."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

try:
    import yaml
except ImportError as exc:
    raise ImportError(
        "pyyaml is required by RemediationKB. Install it with: pip install pyyaml"
    ) from exc

_DEFAULT_KB_PATH = Path(__file__).parent.parent.parent / "data" / "remediation_kb.yaml"


@dataclass
class Remediation:
    title: str
    steps: list[str]
    doc_links: list[str]


class RemediationKB:
    """Load and query the remediation knowledge base.

    Parameters
    ----------
    kb_path:
        Path to the YAML knowledge-base file.  Defaults to
        ``team_project/data/remediation_kb.yaml`` relative to this module.
    """

    def __init__(self, kb_path: Path | str | None = None) -> None:
        path = Path(kb_path) if kb_path is not None else _DEFAULT_KB_PATH
        with open(path) as fh:
            data = yaml.safe_load(fh)
        self._entries: list[dict] = data.get("entries", [])

    def lookup(self, category: str, log_signature: str = "") -> list[Remediation]:
        """Return remediation entries matching *category*.

        Parameters
        ----------
        category:
            One of the classifier category labels (e.g. ``"TRANSIENT"``).
        log_signature:
            Optional log excerpt or keyword string.  When provided, only
            entries whose ``signature`` regex matches this string are
            returned.  Matching is case-insensitive.

        Returns
        -------
        list[Remediation]
            Matching entries in KB order.  Empty list when nothing matches.
        """
        results: list[Remediation] = []
        for entry in self._entries:
            if entry.get("category") != category:
                continue
            if log_signature:
                pattern = entry.get("signature", "")
                if not re.search(pattern, log_signature, re.IGNORECASE):
                    continue
            results.append(
                Remediation(
                    title=entry["title"],
                    steps=entry.get("steps", []),
                    doc_links=entry.get("doc_links", []),
                )
            )
        return results
