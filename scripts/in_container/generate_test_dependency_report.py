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
from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from get_dependency_status import build_dependency_depth_map

OUTPUT_DIR = Path("/files")
FREEZE_FILE_PREFIX = "test-dependencies-freeze"
TREE_FILE_PREFIX = "test-dependencies-tree"
DEPTH_FILE_PREFIX = "test-dependencies-depth"
SUMMARY_FILE_PREFIX = "test-dependencies-summary"


@dataclass(frozen=True)
class CommandResult:
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def succeeded(self) -> bool:
        return self.returncode == 0


def build_report_suffix(job_id: str | None, env: Mapping[str, str] | None = None) -> str:
    env = env or os.environ
    raw_value = (
        job_id
        or env.get("JOB_ID")
        or "-".join(
            value
            for value in (
                env.get("TEST_GROUP"),
                env.get("TEST_TYPE"),
                env.get("PYTHON_MAJOR_MINOR_VERSION"),
            )
            if value
        )
        or "unknown-job"
    )
    normalized_value = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw_value.strip()).strip("-.")
    return normalized_value or "unknown-job"


def build_report_paths(output_dir: Path, suffix: str) -> dict[str, Path]:
    return {
        "freeze": output_dir / f"{FREEZE_FILE_PREFIX}-{suffix}.txt",
        "tree": output_dir / f"{TREE_FILE_PREFIX}-{suffix}.txt",
        "depth": output_dir / f"{DEPTH_FILE_PREFIX}-{suffix}.json",
        "summary": output_dir / f"{SUMMARY_FILE_PREFIX}-{suffix}.md",
    }


def run_command(
    command: tuple[str, ...],
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> CommandResult:
    try:
        completed_process = runner(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        return CommandResult(command=command, returncode=1, stdout="", stderr=str(error))
    return CommandResult(
        command=command,
        returncode=completed_process.returncode,
        stdout=completed_process.stdout,
        stderr=completed_process.stderr,
    )


def _write_command_output(target_path: Path, command_result: CommandResult) -> None:
    target_path.write_text(command_result.stdout, encoding="utf-8")
    if command_result.stderr:
        target_path.with_suffix(f"{target_path.suffix}.stderr").write_text(
            command_result.stderr, encoding="utf-8"
        )


def _format_command_status(command_result: CommandResult) -> str:
    status = "ok" if command_result.succeeded else f"failed ({command_result.returncode})"
    return f"`{' '.join(command_result.command)}`: {status}"


def _summarize_depth_map(depth_map: dict[str, int]) -> dict[str, int]:
    if not depth_map:
        return {"package_count": 0, "root_count": 0, "max_depth": 0}
    return {
        "package_count": len(depth_map),
        "root_count": sum(1 for depth in depth_map.values() if depth == 0),
        "max_depth": max(depth_map.values()),
    }


def _airflow_packages_from_freeze(freeze_output: str) -> list[str]:
    return sorted(line for line in freeze_output.splitlines() if line.startswith("apache-airflow"))


def build_summary(
    *,
    env: Mapping[str, str],
    freeze_result: CommandResult,
    tree_result: CommandResult,
    depth_map: dict[str, int],
    report_paths: Mapping[str, Path],
) -> str:
    metadata_fields = [
        ("Job ID", env.get("JOB_ID", "unknown")),
        ("Test Group", env.get("TEST_GROUP", "")),
        ("Test Type", env.get("TEST_TYPE", "")),
        ("Python", env.get("PYTHON_MAJOR_MINOR_VERSION", "")),
        ("Backend", env.get("BACKEND", "")),
        ("Backend Version", env.get("BACKEND_VERSION", "")),
        ("Use Airflow Version", env.get("USE_AIRFLOW_VERSION", "current-sources")),
        ("Airflow Constraints Ref", env.get("AIRFLOW_CONSTRAINTS_REFERENCE", "")),
        ("Providers Constraints Ref", env.get("PROVIDERS_CONSTRAINTS_REFERENCE", "")),
        ("Force Lowest Dependencies", env.get("FORCE_LOWEST_DEPENDENCIES", "false")),
        ("Upgrade SQLAlchemy", env.get("UPGRADE_SQLALCHEMY", "false")),
        ("Downgrade SQLAlchemy", env.get("DOWNGRADE_SQLALCHEMY", "false")),
        ("Downgrade Pendulum", env.get("DOWNGRADE_PENDULUM", "false")),
        ("Upgrade Boto", env.get("UPGRADE_BOTO", "false")),
    ]
    depth_summary = _summarize_depth_map(depth_map)
    airflow_packages = _airflow_packages_from_freeze(freeze_result.stdout)
    lines = ["## Test Dependency Report", ""]
    lines.extend(f"- {label}: {value or 'n/a'}" for label, value in metadata_fields)
    lines.extend(
        [
            "",
            "### Commands",
            "",
            f"- {_format_command_status(freeze_result)}",
            f"- {_format_command_status(tree_result)}",
            "",
            "### Summary",
            "",
            f"- Packages in dependency depth map: {depth_summary['package_count']}",
            f"- Root packages in dependency tree: {depth_summary['root_count']}",
            f"- Maximum dependency depth: {depth_summary['max_depth']}",
            f"- Airflow packages in freeze: {len(airflow_packages)}",
            "",
            "### Artifacts",
            "",
            f"- Freeze: `{report_paths['freeze'].name}`",
            f"- Tree: `{report_paths['tree'].name}`",
            f"- Depth JSON: `{report_paths['depth'].name}`",
        ]
    )
    if airflow_packages:
        lines.extend(["", "### Airflow Packages", ""])
        lines.extend(f"- `{package}`" for package in airflow_packages)
    if freeze_result.stderr or tree_result.stderr:
        lines.extend(["", "### Command stderr", ""])
        if freeze_result.stderr:
            lines.extend(["```text", freeze_result.stderr.rstrip(), "```"])
        if tree_result.stderr:
            lines.extend(["```text", tree_result.stderr.rstrip(), "```"])
    lines.append("")
    return "\n".join(lines)


def generate_report(output_dir: Path = OUTPUT_DIR, env: Mapping[str, str] | None = None) -> dict[str, Path]:
    env = env or os.environ
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = build_report_suffix(env.get("JOB_ID"), env=env)
    report_paths = build_report_paths(output_dir, suffix)

    freeze_result = run_command(("uv", "pip", "freeze"))
    tree_result = run_command(("uv", "tree", "--no-dedupe"))

    _write_command_output(report_paths["freeze"], freeze_result)
    _write_command_output(report_paths["tree"], tree_result)

    depth_map = build_dependency_depth_map(tree_result.stdout.splitlines()) if tree_result.stdout else {}
    report_paths["depth"].write_text(json.dumps(depth_map, indent=4) + "\n", encoding="utf-8")
    report_paths["summary"].write_text(
        build_summary(
            env=env,
            freeze_result=freeze_result,
            tree_result=tree_result,
            depth_map=depth_map,
            report_paths=report_paths,
        ),
        encoding="utf-8",
    )
    return report_paths


if __name__ == "__main__":
    generate_report()
