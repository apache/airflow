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
# ///
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from collections import defaultdict
from pathlib import Path

PROBE_KEYS = ("component", "profile", "platform", "python")
GRAPH_CHANGE_FIELDS = ("excluded_dependencies", "dependency_overrides", "changed_requirements")
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def read_probe(project: Path) -> tuple[dict, str, list, list, Path]:
    manifest = (project / "pyproject.toml").read_text()
    data = tomllib.loads(manifest)
    metadata = data.get("tool", {}).get("python-version-readiness", {})
    if not all(isinstance(metadata.get(key), str) and metadata[key] for key in PROBE_KEYS):
        raise ValueError(
            "scratch manifest needs tool.python-version-readiness component/profile/platform/python"
        )
    if not re.fullmatch(r"[0-9]+\.[0-9]+", metadata["python"]):
        raise ValueError("probe python must be a major.minor version")
    if data.get("project", {}).get("requires-python") != f"=={metadata['python']}.*":
        raise ValueError("scratch requires-python must equal the target series, e.g. ==3.15.*")
    uv_settings = data.get("tool", {}).get("uv", {})
    if "workspace" not in uv_settings:
        raise ValueError("declare tool.uv.workspace in the scratch root to stop parent-workspace discovery")
    machine = {"linux/amd64": "x86_64", "linux/arm64": "aarch64"}.get(metadata["platform"])
    if machine is None:
        raise ValueError("probe platform must be linux/amd64 or linux/arm64")
    environment = (
        f"python_version == '{metadata['python']}' and sys_platform == 'linux' "
        f"and platform_machine == '{machine}'"
    )
    if uv_settings.get("environments") != [environment]:
        raise ValueError(f"scratch environments must equal [{environment!r}]")
    if uv_settings.get("required-environments") != [environment]:
        raise ValueError("scratch required-environments must match its target environment")
    if (project / "uv.toml").exists():
        raise ValueError("use an explicit resolver config file, not an adjacent uv.toml")
    metadata_keys = {
        "workspace",
        "sources",
        "conflicts",
        "environments",
        "required-environments",
        "package",
        "managed",
        "exclude-dependencies",
        "override-dependencies",
        "constraint-dependencies",
        "build-constraint-dependencies",
        "dependency-groups",
        "dev-dependencies",
    }
    if uv_settings.keys() - metadata_keys:
        raise ValueError("put resolver/index settings in the explicit config file, not tool.uv")
    config_name = metadata.get("config-file")
    if not isinstance(config_name, str) or not config_name:
        raise ValueError("scratch metadata needs config-file naming its explicit resolver configuration")
    config_path = (project / config_name).resolve()
    if config_path.parent != project.resolve():
        raise ValueError("resolver config file must be directly inside the scratch project")
    config_settings = tomllib.loads(config_path.read_text())
    if config_settings.keys() & (metadata_keys | {"frozen", "locked", "no-config", "config-file"}):
        raise ValueError("resolver configuration must not replace project metadata or skip resolution")
    excluded = uv_settings.get("exclude-dependencies", [])
    if not isinstance(excluded, list):
        raise ValueError("exclude-dependencies must be a list")
    overrides = uv_settings.get("override-dependencies", [])
    if not isinstance(overrides, list):
        raise ValueError("override-dependencies must be a list")
    return metadata, manifest, excluded, overrides, config_path


def snapshot_inputs(project: Path) -> tuple[dict, dict, list]:
    manifests = {
        path.relative_to(project).as_posix(): path.read_text()
        for path in sorted(project.rglob("pyproject.toml"))
    }
    baseline = json.loads((project / "baseline-manifests.json").read_text())
    if not isinstance(baseline, dict) or baseline.keys() != manifests.keys():
        raise ValueError("baseline-manifests.json must cover every scratch manifest")
    changes = []
    for path, current in manifests.items():
        before = tomllib.loads(baseline[path])
        after = tomllib.loads(current)
        for key in ("dependencies", "optional-dependencies"):
            original = before.get("project", {}).get(key, [])
            observed = after.get("project", {}).get(key, [])
            if original != observed:
                changes.append({"manifest": path, "field": key, "before": original, "after": observed})
        if before.get("dependency-groups", {}) != after.get("dependency-groups", {}):
            changes.append({"manifest": path, "field": "dependency-groups"})
    return manifests, baseline, changes


def run_command(command: list[str], timeout: float) -> tuple[int, str, str]:
    try:
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("UV_") or key == "UV_CACHE_DIR"
        }
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False, env=environment
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout or b""
        stderr = error.stderr or b""
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        return 124, stdout, f"{stderr}\nProbe timed out after {timeout:g} seconds."
    except OSError as error:
        return 127, "", str(error)


def capture_probe(project: Path, record_path: Path, timeout: float, offline: bool = False) -> int:
    if record_path.exists():
        raise ValueError(f"record already exists: {record_path}; use a new attempt filename")
    project = project.resolve()
    metadata, manifest, excluded, overrides, config_path = read_probe(project)
    manifests, baseline, changed_requirements = snapshot_inputs(project)
    lock_path = project / "uv.lock"
    lock_before = lock_path.read_text() if lock_path.exists() else None
    _, uv_version, version_error = run_command(["uv", "--version"], timeout=10)
    command = [
        "uv",
        "lock",
        "--project",
        str(project),
        "--python",
        metadata["python"],
        "--color",
        "never",
        "--config-file",
        str(config_path),
    ]
    if offline:
        command.extend(["--offline", "--no-python-downloads"])
    code, stdout, stderr = run_command(command, timeout=timeout)
    record = {
        "schema_version": 1,
        **{key: metadata[key] for key in PROBE_KEYS},
        "command": command,
        "uv_version": (uv_version or version_error).strip(),
        "manifest_sha256": hashlib.sha256(manifest.encode()).hexdigest(),
        "manifest": manifest,
        "resolver_config": config_path.read_text(),
        "probe_metadata": metadata,
        "input_manifests": manifests,
        "baseline_manifests": baseline,
        "changed_requirements": changed_requirements,
        "lock_before": lock_before,
        "lock_after": lock_path.read_text() if lock_path.exists() else None,
        "excluded_dependencies": excluded,
        "dependency_overrides": overrides,
        "exit_code": code,
        "stdout": stdout,
        "stderr": stderr,
    }
    record_path.parent.mkdir(parents=True, exist_ok=True)
    with record_path.open("x") as stream:
        stream.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return code if code >= 0 else 1


def escape_cell(value: str) -> str:
    text = " ".join(ANSI_ESCAPE.sub("", value).split())
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("|", "&#124;")


def read_attempt(path: Path, check: dict, target: str) -> dict:
    record = json.loads(path.read_text())
    if record.get("schema_version") != 1:
        raise ValueError(f"unsupported record schema: {path}")
    expected = {"python": target, **{key: check[key] for key in PROBE_KEYS if key != "python"}}
    if any(record.get(key) != value for key, value in expected.items()):
        raise ValueError(f"record target/component/profile/platform does not match plan: {path}")
    if type(record.get("exit_code")) is not int:
        raise ValueError(f"record exit_code must be an integer: {path}")
    for field in GRAPH_CHANGE_FIELDS:
        if not isinstance(record.get(field), list):
            raise ValueError(f"record {field} must be a list: {path}")
    if not all(isinstance(record.get(key), str) for key in ("stdout", "stderr")):
        raise ValueError(f"record stdout/stderr must be strings: {path}")
    return record


def is_probe_ready(record: dict) -> bool:
    return record["exit_code"] == 0 and not any(record[field] for field in GRAPH_CHANGE_FIELDS)


def build_report(plan_path: Path) -> str:
    plan = json.loads(plan_path.read_text())
    target = plan["python"]
    if not isinstance(target, str) or not re.fullmatch(r"[0-9]+\.[0-9]+", target):
        raise ValueError("plan python must be a major.minor version")
    checks = plan["checks"]
    if not isinstance(checks, list) or not checks:
        raise ValueError("plan needs a nonempty checks list, including unprobed components")
    rows: dict[str, list[tuple[bool, str]]] = defaultdict(list)
    seen: set[tuple[str, str, str]] = set()
    for check in sorted(checks, key=lambda item: (item["component"], item["profile"], item["platform"])):
        key = tuple(check[field] for field in PROBE_KEYS if field != "python")
        if not all(isinstance(value, str) and value for value in key) or key in seen:
            raise ValueError("checks need distinct, nonempty component/profile/platform values")
        seen.add(key)
        paths = check["attempts"]
        if not isinstance(paths, list):
            raise ValueError("check attempts must be a list of record paths")
        records = [read_attempt(plan_path.parent / path, check, target) for path in paths]
        ready = bool(records) and all(is_probe_ready(record) for record in records)
        details = [f"{check['profile']} / {check['platform']}: "]
        if not records:
            details.append("not probed")
        elif ready:
            details.append("unmodified resolver probe passed; install/build/tests pending")
        else:
            for path, record in zip(paths, records):
                if is_probe_ready(record):
                    continue
                details.append(f"{path} (exit {record['exit_code']}): ")
                if record["excluded_dependencies"]:
                    details.append(
                        "diagnostically excluded " + json.dumps(record["excluded_dependencies"]) + "; "
                    )
                if record["dependency_overrides"]:
                    details.append(
                        "dependency overrides " + json.dumps(record["dependency_overrides"]) + "; "
                    )
                if record["changed_requirements"]:
                    details.append(
                        "changed probe requirements " + json.dumps(record["changed_requirements"]) + "; "
                    )
                diagnostic = (record["stderr"] + "\n" + record["stdout"]).strip()
                details.append(diagnostic or "uv returned no diagnostic")
                details.append("; ")
        rows[check["component"]].append((ready, escape_cell("".join(details).rstrip("; "))))
    lines = [
        f"# Python {target} dependency readiness",
        "",
        "✅ means every planned resolver probe passed without diagnostic exclusions, dependency overrides, or changed probe requirements. "
        "❌ includes dependency blockers, incomplete probes, and operational failures. "
        "Neither symbol certifies installation, wheel availability, or runtime support.",
        "",
        "| Component | Ready | Dependency details |",
        "| --- | --- | --- |",
    ]
    for component in sorted(rows, key=lambda name: ({"core": 0, "sdk": 1}.get(name, 2), name)):
        cells = rows[component]
        symbol = "✅" if all(ready for ready, _ in cells) else "❌"
        lines.append(
            f"| {escape_cell(component)} | {symbol} | {'<br>'.join(details for _, details in cells)} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture uv lock probes and render a deterministic readiness table."
    )
    commands = parser.add_subparsers(dest="action", required=True)
    capture = commands.add_parser(
        "capture", help="run uv lock against an explicitly marked scratch target probe"
    )
    capture.add_argument("--project", type=Path, required=True)
    capture.add_argument("--record", type=Path, required=True)
    capture.add_argument("--timeout", type=float, default=300)
    capture.add_argument("--offline", action="store_true")
    report = commands.add_parser(
        "report", help="render captured attempts according to an explicit component plan"
    )
    report.add_argument("plan", type=Path)
    report.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.action == "capture":
            if args.timeout <= 0:
                raise ValueError("timeout must be positive")
            return capture_probe(args.project, args.record, args.timeout, args.offline)
        rendered = build_report(args.plan)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
        return 2


if __name__ == "__main__":
    sys.exit(main())
