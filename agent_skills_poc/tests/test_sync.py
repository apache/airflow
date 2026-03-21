# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.generator.generate_skills import generate_skills


def test_docs_and_committed_skills_are_in_sync(tmp_path) -> None:
    """Fail fast in CI when docs and committed skills.json drift apart."""
    project_root = Path(__file__).resolve().parents[1]
    docs_path = project_root / "docs" / "CONTRIBUTING_POC.rst"
    committed_skills_path = project_root / "output" / "skills.json"
    generated_path = tmp_path / "generated_skills.json"

    generate_skills(docs_path, generated_path)

    committed_payload = json.loads(committed_skills_path.read_text(encoding="utf-8"))
    generated_payload = json.loads(generated_path.read_text(encoding="utf-8"))

    assert generated_payload == committed_payload
