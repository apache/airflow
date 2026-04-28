#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pydantic>=2.0",
#   "PyYAML>=6.0",
#   "rich>=13.6.0",
# ]
# ///

# =============================================================================
# Build and structural smoke test for chart/kustomize-overlays/*.
#
# Runs the same checks against every overlay; nothing here is overlay- or
# CRD-specific.
#
# What this hook validates:
#   * `kubectl kustomize` builds the overlay successfully.
#   * The output parses as valid YAML.
#   * At least one resource is produced.
#   * Every resource carries apiVersion, kind, and metadata.name.
#   * No two resources share the same (apiVersion, kind, namespace, name).
#
# What this hook does not validate:
#   * Field schema correctness against the targeted CRD. A typo in a field
#     name will still pass.
#   * Cross-references between resources, or references to resources
#     produced elsewhere (for example by the chart).
#   * Runtime behaviour: the overlay is never applied to a live API server
#     and no controller ever reconciles it.
#
# Treat a passing run as "the overlay is structurally well-formed", not as
# "the overlay works against Kubernetes". An overlay's STATUS file may only
# advance to `tested` once a functional integration test is in place; this
# hook alone is not enough to support that claim. See CONTRIBUTING.rst in
# `chart/kustomize-overlays/` for the lifecycle.
# =============================================================================

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Annotated, Literal

import yaml
from common_prek_utils import AIRFLOW_ROOT_PATH, console, initialize_breeze_prek
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

initialize_breeze_prek(__name__, __file__)


# ---------------------------------------------------------------------------
# STATUS.yaml contract — Pydantic discriminated union, one variant per status.
# ---------------------------------------------------------------------------


class _TestedStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    status: Literal["tested"]
    chart_version: str = Field(alias="chart-version")
    last_verified: str = Field(alias="last-verified", pattern=r"^\d{4}-\d{2}-\d{2}$")


class _NotTestedStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["not-tested"]
    reason: str | None = None


class _DeprecatedStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["deprecated"]
    message: str


_StatusDoc = Annotated[
    _TestedStatus | _NotTestedStatus | _DeprecatedStatus,
    Field(discriminator="status"),
]
_STATUS_ADAPTER: TypeAdapter[_StatusDoc] = TypeAdapter(_StatusDoc)


def _validate_status(overlay_dir: Path) -> list[str]:
    status_path = overlay_dir / "STATUS.yaml"
    if not status_path.exists():
        return [f"missing STATUS.yaml in {overlay_dir.name}"]
    try:
        data = yaml.safe_load(status_path.read_text())
    except yaml.YAMLError as exc:
        return [f"STATUS.yaml is not valid YAML: {exc}"]
    try:
        _STATUS_ADAPTER.validate_python(data)
    except ValidationError as exc:
        return [f"STATUS.yaml schema error: {exc}"]
    return []


def _structural_check(docs: list[object]) -> list[str]:
    """Run generic structural checks that hold for any Kustomize overlay."""
    errors: list[str] = []
    if not docs:
        return ["no resources produced"]

    seen: set[tuple[str, str, str, str]] = set()
    for index, doc in enumerate(docs):
        if not isinstance(doc, dict):
            errors.append(f"document {index} is not a mapping ({type(doc).__name__})")
            continue
        api_version = doc.get("apiVersion") or ""
        kind = doc.get("kind") or ""
        if not api_version:
            errors.append(f"document {index} missing apiVersion")
        if not kind:
            errors.append(f"document {index} missing kind")
        metadata = doc.get("metadata") or {}
        name = metadata.get("name") or ""
        if not name:
            errors.append(f"document {index} missing metadata.name")
        namespace = metadata.get("namespace") or ""
        key = (api_version, kind, namespace, name)
        if key in seen:
            errors.append(f"duplicate resource ({api_version} {kind} {namespace}/{name})")
        seen.add(key)
    return errors


res_setup = subprocess.run(["breeze", "k8s", "setup-env"], check=True)
if res_setup.returncode != 0:
    console.print("[red]\nError while setting up k8s environment.")
    sys.exit(res_setup.returncode)

KUBECTL_BIN_PATH = AIRFLOW_ROOT_PATH / ".venv" / "bin" / "kubectl"
OVERLAYS_DIR = AIRFLOW_ROOT_PATH / "chart" / "kustomize-overlays"

if not OVERLAYS_DIR.is_dir():
    console.print(f"[yellow]No overlay directory at {OVERLAYS_DIR}, nothing to check.")
    sys.exit(0)

kustomizations = sorted(OVERLAYS_DIR.rglob("kustomization.yaml"))
if not kustomizations:
    console.print(f"[yellow]No kustomization.yaml files under {OVERLAYS_DIR}, nothing to check.")
    sys.exit(0)


def _build(overlay_dir: Path) -> tuple[list[object] | None, str]:
    result = subprocess.run(
        [os.fspath(KUBECTL_BIN_PATH), "kustomize", os.fspath(overlay_dir)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None, f"build failed:\n{result.stderr}"
    try:
        docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    except yaml.YAMLError as exc:
        return None, f"build produced invalid YAML: {exc}"
    if result.stderr.strip():
        console.print(f"[yellow]warnings:\n{result.stderr.strip()}")
    return docs, ""


failures: list[str] = []
for kustomization in kustomizations:
    overlay_dir = kustomization.parent
    rel = overlay_dir.relative_to(AIRFLOW_ROOT_PATH)
    console.print(f"[blue]\nKustomize overlay [bold]{rel}[/bold]")

    docs, build_err = _build(overlay_dir)
    if docs is None:
        console.print(f"[red]  {build_err}")
        failures.append(str(rel))
        continue
    console.print(f"[green]  build ok ({len(docs)} resource(s))")

    errors = _structural_check(docs)
    if errors:
        console.print("[red]  structural check failed:")
        for err in errors:
            console.print(f"    - {err}")
        failures.append(str(rel))
    else:
        console.print("[green]  structural check ok")

    status_errors = _validate_status(overlay_dir)
    if status_errors:
        console.print("[red]  STATUS.yaml check failed:")
        for err in status_errors:
            console.print(f"    - {err}")
        if str(rel) not in failures:
            failures.append(str(rel))
    else:
        console.print("[green]  STATUS.yaml ok")

if failures:
    console.print(f"[red]\n{len(failures)} overlay(s) failed:")
    for failure in failures:
        console.print(f"  - {failure}")
    sys.exit(1)

console.print("[green]\nAll Kustomize overlays built and passed the generic structural check.")
console.print(
    "[yellow]Note: this hook does not validate CRD schemas or runtime behaviour. "
    "An overlay's STATUS may only advance to `tested` once a functional integration test exists."
)
