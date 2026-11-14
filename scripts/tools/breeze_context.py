#!/usr/bin/env python3
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
# requires-python = ">=3.11"
# dependencies = [
#     "rich>=13.6.0",
# ]
# ///
"""Detect execution context and route commands between host (uv/breeze) and Breeze container.

This tool acts as a lightweight source of truth for AI agents and human contributors
to inspect the current environment and select the most efficient test and development commands.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path


class ContextType(str, Enum):
    BREEZE = "breeze"
    DEVCONTAINER = "devcontainer"
    HOST = "host"


@dataclass(frozen=True)
class ContextInfo:
    context: str
    in_breeze: bool
    in_container: bool
    can_run_breeze: bool
    repo_root: str
    is_worktree: bool


@dataclass(frozen=True)
class TestCommandRecommendation:
    path: str
    project: str | None
    primary_command: str
    fallback_command: str
    notes: str
    warning: str | None = None


def find_repo_root(start: Path | None = None) -> Path:
    """Find the root of the Airflow git repository."""
    current = (start or Path.cwd()).resolve()
    while current != current.parent:
        if (current / "pyproject.toml").exists() and (current / ".git").exists():
            return current
        current = current.parent

    # Fallback to this script's ancestor directory if current is outside repo
    ancestor = Path(__file__).resolve().parents[2]
    if (ancestor / "pyproject.toml").exists():
        return ancestor
    return (start or Path.cwd()).resolve()


def is_git_worktree(repo_root: Path) -> bool:
    """Check if the repo root is a secondary git worktree."""
    git_entry = repo_root / ".git"
    return git_entry.is_file()


def detect_context(env: dict[str, str] | None = None, repo_root: Path | None = None) -> ContextInfo:
    """Detect whether current execution is in Breeze, a devcontainer, or on the host."""
    environ = os.environ if env is None else env
    root = repo_root or find_repo_root()

    in_breeze = environ.get("BREEZE", "").strip().lower() == "true"

    in_codespaces = environ.get("CODESPACES", "").strip().lower() == "true"
    in_remote_containers = environ.get("REMOTE_CONTAINERS", "").strip().lower() == "true"
    in_docker_container = environ.get("container") == "docker" or Path("/.dockerenv").exists()
    in_devcontainer = not in_breeze and (in_codespaces or in_remote_containers or in_docker_container)

    if in_breeze:
        context = ContextType.BREEZE.value
        in_container = True
        can_run_breeze = False
    elif in_devcontainer:
        context = ContextType.DEVCONTAINER.value
        in_container = True
        can_run_breeze = True
    else:
        context = ContextType.HOST.value
        in_container = False
        can_run_breeze = True

    return ContextInfo(
        context=context,
        in_breeze=in_breeze,
        in_container=in_container,
        can_run_breeze=can_run_breeze,
        repo_root=str(root),
        is_worktree=is_git_worktree(root),
    )


def infer_uv_project(path: str | Path, repo_root: Path | None = None) -> str | None:
    """Infer the UV workspace project corresponding to a file or directory path.

    Returns the project folder relative to repo root (e.g. 'airflow-core',
    'task-sdk', 'providers/amazon', 'scripts') or None if the path does not belong
    to an isolated workspace project.
    """
    root = repo_root or find_repo_root()
    raw_path = Path(path)

    if raw_path.is_absolute():
        try:
            rel_path = raw_path.relative_to(root)
        except ValueError:
            return None
    else:
        rel_path = raw_path

    # Check filesystem: traverse upwards until finding directory with pyproject.toml
    full_path = (root / rel_path).resolve()
    current = full_path if full_path.is_dir() else full_path.parent
    while current != current.parent and current != root:
        if (current / "pyproject.toml").exists():
            return str(current.relative_to(root))
        current = current.parent

    # Pattern check for non-existent paths, relative queries, or hypothetical inputs
    parts = rel_path.parts
    if not parts:
        return None

    if parts[0] in ("airflow-core", "task-sdk", "scripts", "airflow-ctl", "chart", "devel-common"):
        return parts[0]
    if len(parts) >= 2 and parts[0] == "dev" and parts[1] in ("breeze", "mypy", "registry"):
        return f"dev/{parts[1]}"
    if len(parts) >= 2 and parts[0] == "shared":
        return f"shared/{parts[1]}"
    if len(parts) >= 2 and parts[0] == "providers":
        if parts[1] == "apache" and len(parts) >= 3:
            return f"providers/apache/{parts[2]}"
        return f"providers/{parts[1]}"

    return None


def get_test_command(path: str, context: ContextInfo | None = None) -> TestCommandRecommendation:
    """Determine the optimal test command based on execution context and target path."""
    ctx = context or detect_context()
    project = infer_uv_project(path, repo_root=Path(ctx.repo_root))

    if ctx.in_breeze:
        return TestCommandRecommendation(
            path=path,
            project=project,
            primary_command=f"pytest {path} -xvs",
            fallback_command=f"pytest {path} --skip-db-tests -xvs",
            notes="Running inside Breeze container. Pytest runs directly in the container environment.",
            warning="Do not invoke 'breeze run' or 'breeze shell' inside a Breeze container (nested Docker will fail).",
        )

    # Host or Devcontainer environment
    if project:
        return TestCommandRecommendation(
            path=path,
            project=project,
            primary_command=f"uv run --project {project} pytest {path} -xvs",
            fallback_command=f"breeze run pytest {path} -xvs",
            notes="Run locally via uv workspace (fast path). If tests fail with missing system dependencies or require databases, use breeze run.",
            warning=None,
        )

    return TestCommandRecommendation(
        path=path,
        project=None,
        primary_command=f"breeze run pytest {path} -xvs",
        fallback_command="",
        notes="Workspace project could not be inferred; running through breeze on host.",
        warning=None,
    )


def format_summary(ctx: ContextInfo, rec: TestCommandRecommendation | None = None) -> str:
    """Format human-readable summary of context and recommendations."""
    lines = [
        f"Context: {ctx.context.upper()}",
        f"In Breeze container: {ctx.in_breeze}",
        f"Can run breeze: {ctx.can_run_breeze}",
        f"Repo root: {ctx.repo_root}",
        f"Worktree: {ctx.is_worktree}",
    ]
    if rec:
        lines.append("")
        lines.append(f"Target path: {rec.path}")
        if rec.project:
            lines.append(f"UV project: {rec.project}")
        lines.append(f"Primary command: {rec.primary_command}")
        if rec.fallback_command:
            lines.append(f"Fallback command: {rec.fallback_command}")
        lines.append(f"Notes: {rec.notes}")
        if rec.warning:
            lines.append(f"Warning: {rec.warning}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inspect contributor execution context and generate optimal test commands."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output structured JSON instead of human-readable text.",
    )
    parser.add_argument(
        "--path",
        type=str,
        default=None,
        help="Target test or source path to inspect and generate commands for.",
    )
    parser.add_argument(
        "--check-breeze",
        action="store_true",
        help="Exit 0 if running inside Breeze container, 1 otherwise.",
    )

    args = parser.parse_args(argv)

    ctx = detect_context()

    if args.check_breeze:
        if ctx.in_breeze:
            if not args.json:
                print("Running inside Breeze container.")
            return 0
        if not args.json:
            print("Not running inside Breeze container.")
        return 1

    rec = get_test_command(args.path, ctx) if args.path else None

    if args.json:
        data = {
            "context": asdict(ctx),
            "recommendation": asdict(rec) if rec else None,
        }
        print(json.dumps(data, indent=2))
    else:
        print(format_summary(ctx, rec))

    return 0


if __name__ == "__main__":
    sys.exit(main())
