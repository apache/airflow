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
"""Select immutable images from successful trusted main publishers."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

SCHEMA_VERSION = 1
MAX_AGE = timedelta(hours=48)
TRUSTED_REPOSITORY = "apache/airflow"
PUBLISHER_PATH = ".github/workflows/publish-main-images.yml"
KINDS = ("ci", "prod", "prod-dependencies")
# Below this sustained rate, building the image from scratch is assumed faster than finishing
# the download; sized to a compressed image (a few GB) reliably arriving in a couple of minutes.
MIN_DOWNLOAD_SPEED_BYTES_PER_SECOND = 50_000_000  # 50 MB/s
DOWNLOAD_SPEED_PROBE_SECONDS = 30


class DownloadTooSlowError(Exception):
    """A reuse download fell below the speed floor; the consumer should build instead."""


def is_build_input(path: str, kind: str) -> bool:
    """Keep unknown inputs; omit only mounted editable implementations for CI environments."""
    parts = Path(path).parts
    package = parts[0] if parts else ""
    documentation_tree = path.startswith(
        (
            "docs/",
            "docker-stack-docs/",
            "providers-summary-docs/",
            "contributing-docs/",
            "dev/breeze/doc/",
        )
    ) or (
        package in {"airflow-core", "task-sdk", "airflow-ctl", "providers", "shared"}
        and "src" not in parts
        and any(part in {"docs", "newsfragments"} for part in parts[1:-1])
    )
    if documentation_tree or path in {
        "README.md",
        "CODE_OF_CONDUCT.md",
        "COMMITTERS.rst",
        "COMMUNITY_ESCALATION.md",
        "CONTRIBUTING.rst",
        "GOVERNANCE.md",
        "ISSUE_TRIAGE_PROCESS.rst",
        "PROVIDERS.rst",
        "BREEZE.rst",
        "INSTALL",
        "INSTALLING.md",
        "AGENTS.md",
        "CLAUDE.md",
    }:
        return False
    if path.startswith(("chart/", "kubernetes-tests/", "docker-tests/")) or (
        "/tests/" in path and path.startswith(("airflow-core/", "task-sdk/", "airflow-ctl/", "providers/"))
    ):
        return False
    if kind == "prod":
        return True
    file = Path(path)
    # Shared distributions and build-generated metadata remain fingerprinted, even when mounted.
    if (
        path.startswith(("airflow-core/src/", "task-sdk/src/", "airflow-ctl/src/", "providers/"))
        and "/src/" in path
        and file.suffix == ".py"
        and file.name not in {"__init__.py", "get_provider_info.py", "version.py"}
        and "/_generated/" not in path
    ):
        return False
    if path.startswith("airflow-core/src/airflow/ui/src/"):
        return False
    return True


def fingerprint(
    root: Path,
    kind: str,
    python: str,
    platform: str,
    build_args: tuple[str, ...] = (),
    base_image_digest: str = "",
    constraints: bytes = b"",
) -> dict[str, Any]:
    """Hash actual checkout inputs, including file modes, deletions and symlink targets."""
    if kind not in KINDS or platform not in {"linux/amd64", "linux/arm64"}:
        raise ValueError("Unsupported image dimensions")
    dimensions = {
        "schema": SCHEMA_VERSION,
        "kind": kind,
        "python": python,
        "platform": platform,
        "build-args": sorted(build_args),
        "base-image-digest": base_image_digest,
        "constraints-digest": hashlib.sha256(
            b"\n".join(line for line in constraints.splitlines() if not line.lstrip().startswith(b"#"))
        ).hexdigest(),
    }
    digest = hashlib.sha256(json.dumps(dimensions, sort_keys=True).encode())
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
    for name in sorted(set(paths) - {""}):
        if not is_build_input(name, kind):
            continue
        path = root / name
        digest.update(name.encode() + b"\0")
        if path.is_symlink():
            digest.update(b"symlink\0" + os.readlink(path).encode())
        elif path.is_file():
            digest.update(str(path.stat().st_mode & 0o777).encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
        else:
            digest.update(b"deleted")
    return {**dimensions, "fingerprint": digest.hexdigest()}


def artifact_name(inputs: dict[str, Any]) -> str:
    return (
        f"main-image-{inputs['kind']}-{inputs['python']}-"
        f"{inputs['platform'].split('/')[-1]}-{inputs['fingerprint']}"
    )


class GithubArtifacts:
    """Access artifacts without trusting producer-controlled manifest provenance."""

    def __init__(self, repository: str = TRUSTED_REPOSITORY):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("Invalid repository")
        self.repository = repository
        self.session = requests.Session()
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.session.headers.update({"Accept": "application/vnd.github+json"})
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        response = self.session.get(
            f"https://api.github.com/repos/{self.repository}/{path}", params=params, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def find(self, name: str, run_id: int | None = None, prefix: bool = False) -> list[dict[str, Any]]:
        path = f"actions/runs/{run_id}/artifacts" if run_id else "actions/artifacts"
        result: list[dict[str, Any]] = []
        for page in range(1, 101):
            params: dict[str, Any] = {"per_page": 100, "page": page}
            if not run_id:
                params["name"] = name
            artifacts = self.get(path, **params)["artifacts"]
            result.extend(
                item
                for item in artifacts
                if (item["name"].startswith(name) if prefix else item["name"] == name) and not item["expired"]
            )
            if len(artifacts) < 100:
                return sorted(result, key=lambda item: item["created_at"], reverse=True)
        raise ValueError("Artifact listing exceeds the lookup limit")

    def validate(
        self,
        artifact: dict[str, Any],
        name: str,
        *,
        check_freshness: bool = True,
        run_attempt: int | None = None,
    ) -> dict[str, Any]:
        if self.repository != TRUSTED_REPOSITORY or artifact["name"] != name or artifact["expired"]:
            raise ValueError("Artifact is not a trusted main image")
        created = datetime.fromisoformat(artifact["created_at"].replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - created
        if check_freshness and not timedelta(0) <= age <= MAX_AGE:
            raise ValueError("Artifact is outside the freshness window")
        run_path = f"actions/runs/{artifact['workflow_run']['id']}"
        if run_attempt is not None:
            if type(run_attempt) is not int or run_attempt < 1:
                raise ValueError("Invalid publisher run attempt")
            run_path += f"/attempts/{run_attempt}"
        run = self.get(run_path)
        if not (
            run["id"] == artifact["workflow_run"]["id"]
            and run["repository"]["full_name"] == TRUSTED_REPOSITORY
            and run["head_repository"]["full_name"] == TRUSTED_REPOSITORY
            and run["head_branch"] == "main"
            and run["event"] in {"push", "schedule", "workflow_dispatch"}
            and run["path"] == PUBLISHER_PATH
            and run["status"] == "completed"
            and run["conclusion"] == "success"
            and run["head_sha"] == artifact["workflow_run"]["head_sha"]
            and type(run["run_attempt"]) is int
            and run["run_attempt"] >= 1
            and (run_attempt is None or run["run_attempt"] == run_attempt)
        ):
            raise ValueError("Artifact producer is not a successful main publisher")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", artifact.get("digest") or ""):
            raise ValueError("Artifact has no verifiable digest")
        return {
            "hit": True,
            "artifact-id": artifact["id"],
            "artifact-name": name,
            "run-id": run["id"],
            "producer-run-attempt": run["run_attempt"],
            "repository": self.repository,
            "source-sha": run["head_sha"],
            "digest": artifact["digest"],
        }

    @contextmanager
    def archive(self, artifact: dict[str, Any]) -> Iterator[zipfile.ZipFile]:
        with tempfile.TemporaryFile() as payload:
            for attempt in range(3):
                try:
                    payload.seek(0)
                    payload.truncate()
                    digest = hashlib.sha256()
                    downloaded = 0
                    speed_checked = False
                    started = time.monotonic()
                    with self.session.get(
                        f"https://api.github.com/repos/{self.repository}/actions/artifacts/{artifact['id']}/zip",
                        timeout=120,
                        stream=True,
                    ) as response:
                        response.raise_for_status()
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            digest.update(chunk)
                            payload.write(chunk)
                            downloaded += len(chunk)
                            elapsed = time.monotonic() - started
                            if not speed_checked and elapsed >= DOWNLOAD_SPEED_PROBE_SECONDS:
                                speed_checked = True
                                speed = downloaded / elapsed
                                if speed < MIN_DOWNLOAD_SPEED_BYTES_PER_SECOND:
                                    raise DownloadTooSlowError(
                                        f"Download averaged {speed / 1_000_000:.1f} MB/s over the first "
                                        f"{elapsed:.0f}s, below the "
                                        f"{MIN_DOWNLOAD_SPEED_BYTES_PER_SECOND / 1_000_000:.0f} MB/s "
                                        "reuse floor"
                                    )
                    if "sha256:" + digest.hexdigest() != artifact.get("digest"):
                        raise ValueError("Artifact digest mismatch")
                    break
                except requests.RequestException:
                    if attempt == 2:
                        raise
                    time.sleep(2**attempt)
            payload.seek(0)
            with zipfile.ZipFile(payload) as archive:
                yield archive


def resolve(inputs: dict[str, Any], api: GithubArtifacts, disabled: bool = False) -> dict[str, Any]:
    """Return a cache miss on unavailable, incompatible or untrusted artifacts."""
    if disabled:
        return {"hit": False, "reason": "reuse disabled"}
    try:
        name = artifact_name(inputs)
        for artifact in api.find(name):
            try:
                return {**api.validate(artifact, name), **inputs}
            except (ValueError, KeyError, TypeError):
                continue
    except (requests.RequestException, ValueError, KeyError, TypeError):
        return {"hit": False, "reason": "artifact lookup unavailable"}
    return {"hit": False, "reason": "no compatible fresh main artifact"}


def publication_exists(api: GithubArtifacts, name: str, run_id: int) -> bool:
    """Keep an already uploaded immutable publisher artifact when rerunning the same run."""
    run = api.get(f"actions/runs/{run_id}")
    if not (
        api.repository == TRUSTED_REPOSITORY
        and run["repository"]["full_name"] == TRUSTED_REPOSITORY
        and run["head_repository"]["full_name"] == TRUSTED_REPOSITORY
        and run["head_branch"] == "main"
        and run["event"] in {"push", "schedule", "workflow_dispatch"}
        and run["path"] == PUBLISHER_PATH
    ):
        raise ValueError("Publication check requires the main publisher workflow")
    return any(
        artifact["workflow_run"]["id"] == run_id
        and artifact["workflow_run"]["head_sha"] == run["head_sha"]
        and re.fullmatch(r"sha256:[0-9a-f]{64}", artifact.get("digest") or "") is not None
        for artifact in api.find(name, run_id)
    )


def select_local(
    api: GithubArtifacts, artifact_id: int, run_id: int, kind: str, python: str, platform: str
) -> dict[str, Any]:
    """Pin a fallback build to this workflow run, without granting main publisher trust."""
    artifact = api.get(f"actions/artifacts/{artifact_id}")
    run = api.get(f"actions/runs/{run_id}")
    prefix = f"built-{kind}-{python}-{platform.split('/')[-1]}-"
    if not (
        artifact["workflow_run"]["id"] == run_id
        and artifact["workflow_run"]["head_sha"] == run["head_sha"]
        and run["repository"]["full_name"] == api.repository
        and artifact["name"].startswith(prefix)
        and artifact["name"].removeprefix(prefix).isdigit()
        and not artifact["expired"]
        and re.fullmatch(r"sha256:[0-9a-f]{64}", artifact.get("digest") or "")
    ):
        raise ValueError("Built image does not belong to the current workflow run")
    return {
        "hit": True,
        "scope": "current-run",
        "artifact-id": artifact_id,
        "artifact-name": artifact["name"],
        "run-id": run_id,
        "repository": api.repository,
        "source-sha": run["head_sha"],
        "digest": artifact["digest"],
        "kind": kind,
        "python": python,
        "platform": platform,
    }


def download(
    selection: dict[str, Any],
    directory: Path,
    repository: str | None = None,
    run_id: int | None = None,
) -> None:
    """Revalidate immutable provenance and digest immediately before loading an image."""
    api = GithubArtifacts(selection["repository"])
    if selection.get("scope") == "current-run":
        if run_id is None or repository != selection["repository"] or run_id != selection["run-id"]:
            raise ValueError("Local selection does not match the consumer workflow run")
        verified = select_local(
            api,
            int(selection["artifact-id"]),
            run_id,
            selection["kind"],
            selection["python"],
            selection["platform"],
        )
        artifact = api.get(f"actions/artifacts/{int(selection['artifact-id'])}")
    else:
        artifact = api.get(f"actions/artifacts/{int(selection['artifact-id'])}")
        verified = api.validate(
            artifact,
            artifact_name(selection),
            check_freshness=False,
            run_attempt=selection["producer-run-attempt"],
        )
    if any(selection[key] != value for key, value in verified.items()):
        raise ValueError("Selection does not match immutable artifact provenance")
    with api.archive(artifact) as archive:
        for member in archive.infolist():
            destination = (directory / member.filename).resolve()
            if not destination.is_relative_to(directory.resolve()):
                raise ValueError("Unexpected image archive member")
        archive.extractall(directory)


def restore_selection(args: argparse.Namespace) -> dict[str, Any]:
    api = GithubArtifacts(args.repository)
    name = f"selected-{args.kind}-{args.python}-{args.platform.split('/')[-1]}-"
    artifacts = api.find(name, args.run_id, prefix=True)
    artifacts = [
        item
        for item in artifacts
        if item["name"].removeprefix(name).isdigit()
        and 1 <= int(item["name"].removeprefix(name)) <= args.run_attempt
    ]
    artifacts.sort(key=lambda item: int(item["name"].removeprefix(name)), reverse=True)
    if not artifacts:
        if args.require_selection:
            raise ValueError("Required image selection is missing for this workflow run")
        return {"hit": False, "reason": "no selection for this run"}
    with api.archive(artifacts[0]) as archive:
        members = archive.namelist()
        if len(members) != 1 or not members[0].endswith(".json"):
            raise ValueError("Invalid selection archive")
        selection = json.loads(archive.read(members[0]))
    if (selection["kind"], selection["python"], selection["platform"]) != (
        args.kind,
        args.python,
        args.platform,
    ):
        raise ValueError("Selection dimensions do not match consumer")
    try:
        download(selection, args.output_directory, args.repository, args.run_id)
    except DownloadTooSlowError as error:
        return {"hit": False, "reason": str(error)}
    return selection


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "fingerprint",
            "resolve",
            "download",
            "restore-selection",
            "select-local",
            "publication-exists",
        ),
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--kind", choices=KINDS)
    parser.add_argument("--python")
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--repository", default=TRUSTED_REPOSITORY)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fingerprint-file", type=Path)
    parser.add_argument("--selection-file", type=Path)
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument("--artifact-name")
    parser.add_argument("--artifact-id", type=int)
    parser.add_argument("--require-selection", action="store_true")
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--run-attempt", type=int)
    parser.add_argument("--disabled", action="store_true")
    parser.add_argument("--build-arg", action="append", default=[])
    parser.add_argument("--base-image-digest", default="")
    parser.add_argument("--constraints-file", type=Path)
    args = parser.parse_args()
    required = {
        "fingerprint": ("kind", "python", "platform"),
        "resolve": () if args.fingerprint_file else ("kind", "python", "platform"),
        "download": ("selection_file", "output_directory"),
        "select-local": ("artifact_id", "kind", "python", "platform", "run_id"),
        "publication-exists": ("artifact_name", "run_id"),
        "restore-selection": ("kind", "python", "platform", "run_id", "run_attempt", "output_directory"),
    }[args.command]
    missing = ["--" + name.replace("_", "-") for name in required if getattr(args, name) is None]
    if missing:
        parser.error(f"{args.command} requires {', '.join(missing)}")
    result: dict[str, Any]
    if args.command == "publication-exists":
        result = {
            "exists": publication_exists(GithubArtifacts(args.repository), args.artifact_name, args.run_id)
        }
    elif args.command == "select-local":
        result = select_local(
            GithubArtifacts(args.repository),
            args.artifact_id,
            args.run_id,
            args.kind,
            args.python,
            args.platform,
        )
    elif args.command == "restore-selection":
        result = restore_selection(args)
    elif args.command == "download":
        result = json.loads(args.selection_file.read_text())
        download(result, args.output_directory, args.repository, args.run_id)
    else:
        inputs = (
            json.loads(args.fingerprint_file.read_text())
            if args.fingerprint_file
            else fingerprint(
                args.root,
                args.kind,
                args.python,
                args.platform,
                tuple(args.build_arg),
                args.base_image_digest,
                args.constraints_file.read_bytes() if args.constraints_file else b"",
            )
        )
        result = (
            {**inputs, "artifact-name": artifact_name(inputs)}
            if args.command == "fingerprint"
            else resolve(inputs, GithubArtifacts(args.repository), args.disabled)
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
