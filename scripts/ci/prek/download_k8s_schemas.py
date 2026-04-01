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
#   "pyyaml>=6.0.3",
#   "requests>=2.31.0",
#   "rich>=13.6.0",
# ]
# ///
"""Download K8s JSON schemas used by helm chart tests.

Runs ``helm template`` with multiple value combinations to discover all
rendered (apiVersion, kind) pairs, then downloads the matching
standalone-strict JSON schemas from the yannh/kubernetes-json-schema
GitHub repository for every supported Kubernetes version and stores them
in a target directory (typically airflow-site/k8s-schemas for publishing
to https://airflow.apache.org/k8s-schemas/).
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests
import yaml
from common_prek_utils import AIRFLOW_ROOT_PATH, console, read_allowed_kubernetes_versions

KUBERNETES_VERSIONS = read_allowed_kubernetes_versions()
DEFAULT_KUBERNETES_VERSION = KUBERNETES_VERSIONS[0]
CHART_DIR = AIRFLOW_ROOT_PATH / "chart"
BASE_URL_TEMPLATE = (
    "https://api.github.com/repos/yannh/kubernetes-json-schema/contents/v{version}-standalone-strict"
)

# Value combinations that exercise conditional templates (executors, KEDA,
# flower, persistence, pgbouncer, HPA, ingress, network policies,
# priority classes, CronJobs, PDBs, ClusterRoles, etc.)
VALUE_SETS: list[dict] = [
    {},
    {"executor": "CeleryExecutor", "flower": {"enabled": True}},
    {"executor": "CeleryExecutor"},
    {"executor": "KubernetesExecutor"},
    {"executor": "CeleryKubernetesExecutor"},
    {"executor": "LocalExecutor"},
    {"executor": "LocalKubernetesExecutor"},
    {
        "executor": "CeleryExecutor",
        "workers": {"keda": {"enabled": True}},
    },
    {"pgbouncer": {"enabled": True}},
    {
        "dags": {"persistence": {"enabled": True}},
        "logs": {"persistence": {"enabled": True}},
    },
    {"redis": {"enabled": True}},
    {"statsd": {"enabled": True}},
    {
        "webserver": {"defaultUser": {"enabled": True}},
        "cleanup": {"enabled": True},
        "databaseCleanup": {"enabled": True},
    },
    {
        "ingress": {"web": {"enabled": True}, "flower": {"enabled": True}},
        "networkPolicies": {"enabled": True},
        "flower": {"enabled": True},
        "executor": "CeleryExecutor",
    },
    {
        "workers": {"hpa": {"enabled": True}},
        "webserver": {
            "hpa": {"enabled": True},
            "podDisruptionBudget": {"enabled": True},
        },
        "scheduler": {"podDisruptionBudget": {"enabled": True}},
    },
    {
        "multiNamespaceMode": True,
        "limits": [{"type": "Container", "max": {"cpu": "2"}}],
        "quotas": {"pods": "10"},
    },
    {
        "priorityClasses": [
            {"name": "high", "preemptionPolicy": "PreemptLowerPriority", "value": 1000},
        ],
    },
]

# Additional (apiVersion, kind) pairs that are needed but not produced by
# ``helm template`` on the main chart (e.g. the pod-template-file is only
# copied into templates/ during tests).
EXTRA_PAIRS: set[tuple[str, str]] = {
    ("v1", "Pod"),
}


def schema_filename(api_version: str, kind: str) -> str:
    """Compute the schema filename using the same logic as ``get_schema_k8s``."""
    api_version = api_version.lower()
    kind = kind.lower()
    if "/" in api_version:
        ext, _, ver = api_version.partition("/")
        ext = ext.split(".")[0]
        return f"{kind}-{ext}-{ver}.json"
    return f"{kind}-{api_version}.json"


def discover_pairs() -> set[tuple[str, str]]:
    """Run helm template with each value set and collect (apiVersion, kind) pairs."""
    pairs: set[tuple[str, str]] = set()
    for values in VALUE_SETS:
        with NamedTemporaryFile(mode="w", suffix=".yaml") as tmp:
            yaml.dump(values, tmp)
            tmp.flush()
            result = subprocess.run(
                [
                    "helm",
                    "template",
                    "release-name",
                    str(CHART_DIR),
                    "--values",
                    tmp.name,
                    "--kube-version",
                    DEFAULT_KUBERNETES_VERSION,
                ],
                capture_output=True,
                check=False,
            )
            if result.returncode != 0:
                console.print(
                    f"[yellow]helm template failed for values {values}: {result.stderr.decode()[:200]}[/]"
                )
                continue
            for obj in yaml.safe_load_all(result.stdout):
                if obj and "apiVersion" in obj and "kind" in obj:
                    pairs.add((obj["apiVersion"], obj["kind"]))
    pairs.update(EXTRA_PAIRS)
    return pairs


def download_schema(base_url: str, filename: str, token: str | None, retries: int = 3) -> str | None:
    """Download a single schema file from GitHub. Returns content or None."""
    import time

    url = f"{base_url}/{filename}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    for attempt in range(retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 404:
            console.print(f"[yellow]  Schema not found (404): {filename}[/]")
            return None
        if resp.status_code >= 500 and attempt < retries - 1:
            wait = 2**attempt
            console.print(f"[yellow]  Server error {resp.status_code}, retrying in {wait}s...[/]")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        # Replace references to kubernetesjsonschema.dev with the raw GitHub URL
        return resp.text.replace(
            "kubernetesjsonschema.dev",
            "raw.githubusercontent.com/yannh/kubernetes-json-schema/master",
        )
    return None


def download_schemas_for_version(
    version: str,
    filenames: dict[str, tuple[str, str]],
    token: str | None,
    output_dir: Path,
) -> tuple[int, int]:
    """Download all schema files for a given K8s version. Returns (downloaded, skipped)."""
    schema_dir = output_dir / f"v{version}-standalone-strict"
    base_url = BASE_URL_TEMPLATE.format(version=version)
    console.print(f"[bold]  Downloading {len(filenames)} schemas for v{version} to {schema_dir}[/]")
    schema_dir.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped = 0
    for fname in sorted(filenames):
        api_version, kind = filenames[fname]
        target = schema_dir / fname
        console.print(f"    {api_version}/{kind} -> {fname}")
        content = download_schema(base_url, fname, token)
        if content is None:
            skipped += 1
            continue
        # Validate it's proper JSON
        json.loads(content)
        target.write_text(content)
        downloaded += 1

    return downloaded, skipped


def main() -> None:
    import os

    parser = argparse.ArgumentParser(description="Download K8s JSON schemas for helm chart tests.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write schemas to (e.g. airflow-site/k8s-schemas).",
    )
    parser.add_argument(
        "--versions",
        nargs="+",
        default=None,
        help="Specific K8s versions to download (default: all ALLOWED_KUBERNETES_VERSIONS).",
    )
    args = parser.parse_args()

    output_dir: Path = args.output_dir
    versions: list[str] = args.versions if args.versions else KUBERNETES_VERSIONS

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        # Try gh CLI
        try:
            result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=False)
            if result.returncode == 0 and result.stdout.strip():
                token = result.stdout.strip()
        except FileNotFoundError:
            pass

    if token:
        console.print("[green]Using GitHub token for authenticated requests.[/]")
    else:
        console.print("[yellow]No GitHub token found. Using unauthenticated requests (60 req/hr limit).[/]")

    console.print("[bold]Discovering (apiVersion, kind) pairs via helm template...[/]")
    pairs = discover_pairs()
    console.print(f"[green]Found {len(pairs)} unique (apiVersion, kind) pairs.[/]")

    # Compute filenames
    filenames: dict[str, tuple[str, str]] = {}
    for api_version, kind in sorted(pairs):
        fname = schema_filename(api_version, kind)
        filenames[fname] = (api_version, kind)

    total_downloaded = 0
    total_skipped = 0
    for version in versions:
        downloaded, skipped = download_schemas_for_version(version, filenames, token, output_dir)
        total_downloaded += downloaded
        total_skipped += skipped

    console.print(
        f"[green]Done. Downloaded {total_downloaded} schemas across "
        f"{len(versions)} versions, skipped {total_skipped}.[/]"
    )


if __name__ == "__main__":
    main()
