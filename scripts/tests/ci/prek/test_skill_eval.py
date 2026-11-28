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

import importlib.util
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
MODULE_PATH = REPO_ROOT / "dev" / "skill-evals" / "eval.py"


@pytest.fixture(scope="module")
def skill_eval_module():
    spec = importlib.util.spec_from_file_location("skill_eval", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        pytest.param(None, "claude", id="default"),
        pytest.param("Claude", "claude", id="case-insensitive"),
        pytest.param("codex", "codex", id="codex"),
    ],
)
def test_get_runtime(monkeypatch, skill_eval_module, configured, expected):
    if configured is None:
        monkeypatch.delenv("AGENT_RUNTIME", raising=False)
    else:
        monkeypatch.setenv("AGENT_RUNTIME", configured)

    assert skill_eval_module.get_runtime() == expected


def test_get_runtime_rejects_unknown_runtime(monkeypatch, skill_eval_module):
    monkeypatch.setenv("AGENT_RUNTIME", "unknown")

    with pytest.raises(ValueError, match="claude, codex"):
        skill_eval_module.get_runtime()


def test_build_codex_provider(skill_eval_module, tmp_path):
    working_dir = tmp_path / "worktree"

    provider = skill_eval_module.build_codex_provider(
        "working", working_dir, "gpt-test", detect_skill_usage=True
    )

    assert provider == {
        "id": "openai:codex-sdk",
        "label": "working",
        "config": {
            "approval_policy": "never",
            "cli_config": {
                "history": {"persistence": "none"},
                "project_doc_max_bytes": skill_eval_module.CODEX_PROJECT_DOC_MAX_BYTES,
            },
            "enable_streaming": True,
            "model": "gpt-test",
            "network_access_enabled": False,
            "output_schema": skill_eval_module.OUTPUT_FORMAT["schema"],
            "sandbox_mode": "read-only",
            "web_search_mode": "disabled",
            "working_dir": str(working_dir),
        },
        "transform": "JSON.parse(output)",
    }


def test_output_schema_requires_every_property(skill_eval_module):
    schema = skill_eval_module.OUTPUT_FORMAT["schema"]

    assert set(schema["required"]) == set(schema["properties"])


def test_build_codex_provider_uses_runtime_default_model(skill_eval_module, tmp_path):
    provider = skill_eval_module.build_codex_provider("main", tmp_path / "worktree", None)

    assert "model" not in provider["config"]
    assert "enable_streaming" not in provider["config"]


@pytest.fixture
def promptfoo_layout(monkeypatch, skill_eval_module, tmp_path):
    """Build a prek-style node env and return an SDK installer for either location.

    ``install_root`` mirrors a package listed in the hook's
    additional_dependencies; ``bundled`` mirrors promptfoo's own optional
    dependency, nested inside its package.
    """
    install_root = tmp_path / "node_modules"
    promptfoo = install_root / "promptfoo"
    bundled = promptfoo / "node_modules"
    promptfoo.mkdir(parents=True)
    (promptfoo / "package.json").write_text("{}")
    promptfoo_bin = promptfoo / "promptfoo"
    promptfoo_bin.touch()
    monkeypatch.setattr(skill_eval_module.shutil, "which", lambda _: str(promptfoo_bin))
    monkeypatch.setattr(
        skill_eval_module,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=f"{skill_eval_module.PROMPTFOO_VERSION}\n"),
    )

    def install(location: Path, package: str) -> None:
        sdk = location.joinpath(*package.split("/"))
        sdk.mkdir(parents=True)
        (sdk / "package.json").write_text('{"version": "1.2.3"}')

    return SimpleNamespace(install_root=install_root, bundled=bundled, install=install)


@pytest.mark.parametrize("runtime", ["claude", "codex"])
def test_find_sdk_modules_resolves_the_hook_installed_copy(skill_eval_module, promptfoo_layout, runtime):
    promptfoo_layout.install(promptfoo_layout.install_root, skill_eval_module.SDK_PACKAGES[runtime])

    assert skill_eval_module.find_sdk_modules(runtime) == promptfoo_layout.install_root


@pytest.mark.parametrize("runtime", ["claude", "codex"])
def test_find_sdk_modules_ignores_the_copy_bundled_with_promptfoo(
    skill_eval_module, promptfoo_layout, runtime
):
    promptfoo_layout.install(promptfoo_layout.bundled, skill_eval_module.SDK_PACKAGES[runtime])

    with pytest.raises(SystemExit) as exit_info:
        skill_eval_module.find_sdk_modules(runtime)

    assert exit_info.value.code == 1


def test_find_sdk_modules_requires_the_selected_runtimes_sdk(skill_eval_module, promptfoo_layout):
    promptfoo_layout.install(promptfoo_layout.install_root, skill_eval_module.SDK_PACKAGES["claude"])

    with pytest.raises(SystemExit) as exit_info:
        skill_eval_module.find_sdk_modules("codex")

    assert exit_info.value.code == 1


def test_find_sdk_modules_runs_version_check_with_repo_local_config(
    monkeypatch, skill_eval_module, promptfoo_layout
):
    promptfoo_layout.install(promptfoo_layout.install_root, skill_eval_module.SDK_PACKAGES["claude"])
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(stdout=f"{skill_eval_module.PROMPTFOO_VERSION}\n")

    monkeypatch.setattr(skill_eval_module, "run", fake_run)

    skill_eval_module.find_sdk_modules("claude")

    assert calls[0]["env"]["PROMPTFOO_CONFIG_DIR"] == str(skill_eval_module.PROMPTFOO_STATE_DIR)


@pytest.mark.parametrize(
    ("runtime", "token_file_content", "expected"),
    [
        pytest.param("claude", None, {}, id="unset"),
        pytest.param("claude", "tok-123\n", {"CLAUDE_CODE_OAUTH_TOKEN": "tok-123"}, id="claude"),
        pytest.param("codex", "", {}, id="codex-ignores-file"),
    ],
)
def test_read_claude_token_env(
    monkeypatch, skill_eval_module, tmp_path, runtime, token_file_content, expected
):
    if token_file_content is None:
        monkeypatch.delenv("SKILL_EVAL_CLAUDE_TOKEN_FILE", raising=False)
    else:
        token_file = tmp_path / "token"
        token_file.write_text(token_file_content)
        monkeypatch.setenv("SKILL_EVAL_CLAUDE_TOKEN_FILE", str(token_file))

    assert skill_eval_module.read_claude_token_env(runtime) == expected


@pytest.mark.parametrize("token_file_content", [None, "  \n"], ids=["missing", "empty"])
def test_read_claude_token_env_rejects_unusable_file(
    monkeypatch, skill_eval_module, tmp_path, token_file_content
):
    token_file = tmp_path / "token"
    if token_file_content is not None:
        token_file.write_text(token_file_content)
    monkeypatch.setenv("SKILL_EVAL_CLAUDE_TOKEN_FILE", str(token_file))

    with pytest.raises(SystemExit) as exit_info:
        skill_eval_module.read_claude_token_env("claude")

    assert exit_info.value.code == 1


@pytest.fixture
def source_repo(monkeypatch, skill_eval_module, tmp_path):
    """A repo tracking AGENTS.md and a CLAUDE.md symlink, plus a global post-checkout hook."""
    hooks = tmp_path / "hooks"
    hooks.mkdir()
    marker = tmp_path / "hook-ran"
    hook = hooks / "post-checkout"
    hook.write_text(f"#!/bin/sh\ntouch {marker}\n")
    hook.chmod(0o755)
    global_config = tmp_path / "gitconfig"
    global_config.write_text(f"[core]\n\thooksPath = {hooks}\n")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(global_config))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "--quiet")
    (repo / "AGENTS.md").write_text("main guidance\n")
    (repo / "CLAUDE.md").symlink_to("AGENTS.md")
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "--quiet", "-m", "init")
    monkeypatch.setattr(skill_eval_module, "REPO_ROOT", repo)
    return SimpleNamespace(path=repo, sha=git("rev-parse", "HEAD"), marker=marker)


def test_create_arm_checks_out_base_without_touching_source_repo(skill_eval_module, source_repo, tmp_path):
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    agents_file = tmp_path / "working-agents.md"
    agents_file.write_text("working guidance\n")

    arm = skill_eval_module.create_arm(work_dir, "working", source_repo.sha, agents_file)

    head = subprocess.run(
        ["git", "-C", str(arm), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    assert head == source_repo.sha
    assert (arm / "AGENTS.md").read_text() == "working guidance\n"
    assert not (arm / "CLAUDE.md").exists()
    assert not (arm / "CLAUDE.md").is_symlink()
    assert not (source_repo.path / ".git" / "worktrees").exists()
    assert not source_repo.marker.exists()


def test_create_arm_baseline_has_no_guidance(skill_eval_module, source_repo, tmp_path):
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    arm = skill_eval_module.create_arm(work_dir, "baseline", source_repo.sha, None)

    assert not (arm / "AGENTS.md").exists()
    assert not (arm / "CLAUDE.md").is_symlink()
