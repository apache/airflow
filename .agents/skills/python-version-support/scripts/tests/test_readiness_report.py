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

# ruff: noqa: S101
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[1] / "readiness_report.py"
SPEC = importlib.util.spec_from_file_location("readiness_report", SCRIPT_PATH)
readiness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(readiness)


def write_record(
    directory, name, component="core", code=0, exclusions=None, diagnostic="", platform="linux/amd64"
):
    record = {
        "schema_version": 1,
        "python": "3.15",
        "component": component,
        "profile": "runtime",
        "platform": platform,
        "exit_code": code,
        "excluded_dependencies": exclusions or [],
        "dependency_overrides": [],
        "changed_requirements": [],
        "stdout": "",
        "stderr": diagnostic,
    }
    (directory / name).write_text(json.dumps(record))
    return name


def write_plan(directory, checks, target="3.15"):
    plan = directory / "plan.json"
    plan.write_text(json.dumps({"python": target, "checks": checks}))
    return plan


def make_check(component, attempts, platform="linux/amd64"):
    return {"component": component, "profile": "runtime", "platform": platform, "attempts": attempts}


def test_report_keeps_excluded_graph_red_and_retains_dependency_chain(tmp_path):
    core = write_record(tmp_path, "core.json")
    sdk = write_record(tmp_path, "sdk.json", component="sdk")
    failed = write_record(
        tmp_path,
        "provider-1.json",
        component="provider-example",
        code=1,
        diagnostic="\x1b[31mBecause direct-package depends on leaf-package<2 | Python<3.15.\x1b[0m",
    )
    weakened = write_record(
        tmp_path, "provider-2.json", component="provider-example", exclusions=["leaf-package"]
    )
    checks = [
        make_check("provider-example", [failed, weakened]),
        make_check("sdk", [sdk]),
        make_check("core", [core]),
        make_check("provider-unprobed", []),
    ]
    result = readiness.build_report(write_plan(tmp_path, checks))
    assert "| core | ✅ |" in result
    assert "| sdk | ✅ |" in result
    assert "| provider-example | ❌ |" in result
    assert "| provider-unprobed | ❌ |" in result
    assert "direct-package depends on leaf-package&lt;2 &#124; Python&lt;3.15" in result
    assert "diagnostically excluded" in result
    assert "\x1b" not in result
    assert result == readiness.build_report(write_plan(tmp_path, list(reversed(checks))))


@pytest.mark.parametrize("diagnostic", ["DNS lookup failed", "unknown failure", ""])
def test_failed_or_ambiguous_probe_never_becomes_green(tmp_path, diagnostic):
    record = write_record(tmp_path, "failed.json", code=1, diagnostic=diagnostic)
    result = readiness.build_report(write_plan(tmp_path, [make_check("core", [record])]))
    assert "| core | ❌ |" in result
    assert (diagnostic or "uv returned no diagnostic") in result


def test_all_architecture_checks_must_pass(tmp_path):
    amd = write_record(tmp_path, "amd.json")
    arm = write_record(tmp_path, "arm.json", code=1, platform="linux/arm64", diagnostic="missing ARM wheel")
    result = readiness.build_report(
        write_plan(tmp_path, [make_check("core", [amd]), make_check("core", [arm], platform="linux/arm64")])
    )
    assert "| core | ❌ |" in result
    assert "missing ARM wheel" in result


@pytest.mark.parametrize(
    ("component", "platform", "target"),
    [("sdk", "linux/amd64", "3.15"), ("core", "linux/arm64", "3.15"), ("core", "linux/amd64", "3.14")],
)
def test_rejects_misattributed_evidence(tmp_path, component, platform, target):
    record = write_record(tmp_path, "core.json")
    plan = write_plan(tmp_path, [make_check(component, [record], platform)], target=target)
    with pytest.raises(ValueError, match="does not match plan"):
        readiness.build_report(plan)


def write_probe(directory, exclusions=False):
    target = f"{sys.version_info.major}.{sys.version_info.minor}"
    directory.mkdir()
    manifest = f'''[project]
name = "readiness-test-probe"
version = "0.0.0"
requires-python = "=={target}.*"
dependencies = ["airflow-readiness-test-missing-package==0.0.0"]
[tool.uv]
package = false
environments = ["python_version == '{target}' and sys_platform == 'linux' and platform_machine == 'x86_64'"]
required-environments = ["python_version == '{target}' and sys_platform == 'linux' and platform_machine == 'x86_64'"]
exclude-dependencies = {json.dumps(["airflow-readiness-test-missing-package"] if exclusions else [])}
[tool.uv.workspace]
members = []
[tool.python-version-readiness]
component = "provider-example"
profile = "runtime"
platform = "linux/amd64"
python = "{target}"
config-file = "resolver.uv.toml"
'''
    (directory / "resolver.uv.toml").write_text("no-index = true\n")
    (directory / "pyproject.toml").write_text(manifest)
    (directory / "baseline-manifests.json").write_text(json.dumps({"pyproject.toml": manifest}))
    return target


def test_real_uv_diagnostic_and_excluded_retry_are_captured(tmp_path):
    original = tmp_path / "original"
    target = write_probe(original)
    original_manifest = (original / "pyproject.toml").read_text()
    failed_record = tmp_path / "failed.json"
    assert readiness.capture_probe(original, failed_record, timeout=30, offline=True) != 0
    failure = json.loads(failed_record.read_text())
    assert "airflow-readiness-test-missing-package" in failure["stderr"]
    assert failure["manifest"] == original_manifest
    assert (original / "pyproject.toml").read_text() == original_manifest
    weakened = tmp_path / "weakened"
    write_probe(weakened, exclusions=True)
    success_record = tmp_path / "weakened.json"
    assert readiness.capture_probe(weakened, success_record, timeout=30, offline=True) == 0
    plan = write_plan(tmp_path, [make_check("provider-example", ["failed.json", "weakened.json"])], target)
    assert "| provider-example | ❌ |" in readiness.build_report(plan)
    with pytest.raises(ValueError, match="already exists"):
        readiness.capture_probe(weakened, success_record, timeout=30, offline=True)


@pytest.mark.parametrize("section", ["[tool.python-version-readiness]", "[tool.uv.workspace]"])
def test_capture_rejects_unmarked_or_parent_workspace_probe(tmp_path, section):
    project = tmp_path / "probe"
    write_probe(project)
    manifest = project / "pyproject.toml"
    manifest.write_text(manifest.read_text().replace(section, section.replace("tool.", "tool.ignored.")))
    with pytest.raises(ValueError, match="scratch|declare"):
        readiness.capture_probe(project, tmp_path / "record.json", timeout=30, offline=True)
    assert not (tmp_path / "record.json").exists()


def test_dependency_override_does_not_earn_unmodified_readiness(tmp_path):
    path = tmp_path / write_record(tmp_path, "override.json")
    record = json.loads(path.read_text())
    record["dependency_overrides"] = ["leaf-package>=2"]
    path.write_text(json.dumps(record))
    result = readiness.build_report(write_plan(tmp_path, [make_check("core", [path.name])]))
    assert "| core | ❌ |" in result
    assert "leaf-package&gt;=2" in result


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ('platform = "linux/amd64"', 'platform = "linux/arm64"', "environments"),
        ('requires-python = "==', 'requires-python = ">=', "target series"),
        ("required-environments = [", "wrong-required-environments = [", "required-environments"),
        ('python = "', 'python = "invalid-', "major.minor"),
        ('config-file = "resolver.uv.toml"', 'config-file = "../outside.toml"', "directly inside"),
    ],
)
def test_capture_refuses_target_or_configuration_mismatch(tmp_path, old, new, message):
    project = tmp_path / "probe"
    write_probe(project)
    manifest = project / "pyproject.toml"
    manifest.write_text(manifest.read_text().replace(old, new))
    with pytest.raises(ValueError, match=message):
        readiness.capture_probe(project, tmp_path / "record.json", timeout=30, offline=True)


def test_inherited_frozen_cannot_turn_stale_lock_into_success(tmp_path, monkeypatch):
    project = tmp_path / "probe"
    write_probe(project, exclusions=True)
    assert readiness.capture_probe(project, tmp_path / "baseline.json", timeout=30, offline=True) == 0
    manifest = project / "pyproject.toml"
    manifest.write_text(manifest.read_text().replace('["airflow-readiness-test-missing-package"]', "[]"))
    monkeypatch.setenv("UV_FROZEN", "true")
    monkeypatch.setenv("UV_NO_CONFIG", "true")
    assert readiness.capture_probe(project, tmp_path / "failed.json", timeout=30, offline=True) != 0
    record = json.loads((tmp_path / "failed.json").read_text())
    assert "airflow-readiness-test-missing-package" in record["stderr"]
    assert "no-index = true" in record["resolver_config"]


@pytest.mark.parametrize(
    ("filename", "config"), [("uv.toml", "no-index = true"), ("resolver.uv.toml", "frozen = true")]
)
def test_conflicting_configurations_are_rejected(tmp_path, filename, config):
    project = tmp_path / "probe"
    write_probe(project)
    (project / filename).write_text(config)
    with pytest.raises(ValueError, match="adjacent|skip resolution"):
        readiness.capture_probe(project, tmp_path / "record.json", timeout=30, offline=True)


def test_timeout_preserves_partial_diagnostics():
    code, stdout, stderr = readiness.run_command(
        [sys.executable, "-c", "import time; print('partial diagnostic', flush=True); time.sleep(2)"],
        timeout=0.1,
    )
    assert code == 124
    assert "partial diagnostic" in stdout
    assert "timed out" in stderr


def test_missing_uv_is_a_failure():
    code, _, diagnostic = readiness.run_command(["airflow-readiness-test-nonexistent-executable"], timeout=1)
    assert code == 127
    assert diagnostic


def test_deleting_requirements_is_not_reported_as_ready(tmp_path):
    project = tmp_path / "probe"
    target = write_probe(project)
    manifest = project / "pyproject.toml"
    manifest.write_text(
        manifest.read_text().replace('["airflow-readiness-test-missing-package==0.0.0"]', "[]")
    )
    record_path = tmp_path / "deleted.json"
    assert readiness.capture_probe(project, record_path, timeout=30, offline=True) == 0
    record = json.loads(record_path.read_text())
    assert record["baseline_manifests"]["pyproject.toml"] != record["input_manifests"]["pyproject.toml"]
    assert record["lock_after"]
    plan = write_plan(tmp_path, [make_check("provider-example", [record_path.name])], target)
    report = readiness.build_report(plan)
    assert "| provider-example | ❌ |" in report
    assert "changed probe requirements" in report
    assert "airflow-readiness-test-missing-package" in report


def test_incomplete_baseline_cannot_claim_reproducibility(tmp_path):
    project = tmp_path / "probe"
    write_probe(project)
    (project / "baseline-manifests.json").write_text("{}")
    with pytest.raises(ValueError, match="cover every scratch manifest"):
        readiness.capture_probe(project, tmp_path / "record.json", timeout=30, offline=True)
