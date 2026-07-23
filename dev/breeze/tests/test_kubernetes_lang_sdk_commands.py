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

from unittest import mock

import pytest

from airflow_breeze.commands import kubernetes_commands
from airflow_breeze.commands.kubernetes_commands import (
    _lang_sdk_build_go_bundle,
    _lang_sdk_build_java_jar,
    _lang_sdk_fetch_upstream_sdk_sources,
    _lang_sdk_upload_artifacts,
)
from airflow_breeze.utils import shared_options


@pytest.fixture
def dry_run(monkeypatch):
    monkeypatch.setattr(shared_options._SharedOptions, "dry_run_value", True)


@pytest.fixture
def go_example(tmp_path, monkeypatch):
    """Point the go_example dir at a tmp path (under a tmp repo root) with a pre-built bundle binary."""
    monkeypatch.setattr(kubernetes_commands, "AIRFLOW_ROOT_PATH", tmp_path)
    go_dir = tmp_path / "go_example"
    (go_dir / "bin").mkdir(parents=True)
    (go_dir / "bin" / kubernetes_commands.LANG_SDK_GO_BUNDLE_NAME).write_text("binary")
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_GO_EXAMPLE_PATH", go_dir)
    return go_dir


@pytest.fixture
def upstream_go_sdk(tmp_path):
    """A fake extracted upstream-main go-sdk copy, distinguishable from anything under go_example."""
    sdk_dir = tmp_path / "upstream_go_sdk"
    sdk_dir.mkdir()
    (sdk_dir / "marker.go").write_text("upstream")
    return sdk_dir


@pytest.fixture
def java_example(tmp_path, monkeypatch):
    """Point the java_example dir at a tmp path (under a tmp repo root) with a pre-built bundle jar."""
    monkeypatch.setattr(kubernetes_commands, "AIRFLOW_ROOT_PATH", tmp_path)
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_MAVEN_CACHE_PATH", tmp_path / "m2")
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_GRADLE_CACHE_PATH", tmp_path / "gradle")
    java_dir = tmp_path / "java_example"
    (java_dir / "build" / "bundle").mkdir(parents=True)
    (java_dir / "build" / "bundle" / "app.jar").write_text("jar")
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_JAVA_EXAMPLE_PATH", java_dir)
    return java_dir


@pytest.fixture
def upstream_java_sdk(tmp_path):
    """A fake extracted upstream-main java-sdk copy, distinguishable from the real java-sdk/."""
    sdk_dir = tmp_path / "upstream_java_sdk"
    sdk_dir.mkdir()
    (sdk_dir / "marker.gradle").write_text("upstream")
    return sdk_dir


class TestLangSdkBuildGoBundle:
    @mock.patch.object(kubernetes_commands, "run_command")
    def test_native_uses_host_go_toolchain(self, mock_run, tmp_path, go_example, upstream_go_sdk):
        _lang_sdk_build_go_bundle(tmp_path, upstream_go_sdk, None, native=True)

        cmd = mock_run.call_args.args[0]
        assert cmd[:3] == ["go", "tool", "airflow-go-pack"]
        assert "docker" not in cmd
        workspace_example = mock_run.call_args.kwargs["cwd"]
        # The build runs against a scratch workspace copy, not the real go_example dir.
        assert workspace_example != go_example
        assert workspace_example.name == go_example.name
        assert mock_run.call_args.kwargs["env"]["CGO_ENABLED"] == "0"
        # The workspace mirrors the repo layout, with go-sdk swapped for the upstream copy.
        assert (workspace_example.parent / "go-sdk" / "marker.go").read_text() == "upstream"
        assert (tmp_path / "go-artifacts" / kubernetes_commands.LANG_SDK_GO_BUNDLE_NAME).exists()

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_container_mode_runs_in_docker(self, mock_run, tmp_path, go_example, upstream_go_sdk):
        _lang_sdk_build_go_bundle(tmp_path, upstream_go_sdk, None, native=False)

        cmd = mock_run.call_args.args[0]
        assert cmd[0] == "docker"
        assert kubernetes_commands.LANG_SDK_GO_BUILDER_IMAGE in cmd
        # The workspace, not AIRFLOW_ROOT_PATH, is mounted at /repo; the persistent HOME cache
        # dir comes from the real go_example.
        mounts = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-v"]
        repo_mount = next(m for m in mounts if m.endswith(":/repo"))
        assert repo_mount.split(":")[0] != str(go_example.parent)
        home_mount = next(m for m in mounts if m.endswith("/.home"))
        assert home_mount.startswith(str(go_example / ".home"))


class TestLangSdkBuildJavaJar:
    @mock.patch.object(kubernetes_commands, "run_command")
    def test_native_uses_host_gradle_toolchain(self, mock_run, tmp_path, java_example, upstream_java_sdk):
        _lang_sdk_build_java_jar(tmp_path, upstream_java_sdk, None, native=True)

        publish_cmd, bundle_cmd = (call.args[0] for call in mock_run.call_args_list)
        assert publish_cmd == [
            "./gradlew",
            "publishToMavenLocal",
            "-PskipSigning=true",
            "--no-daemon",
            "--console=plain",
        ]
        assert bundle_cmd == [
            "./gradlew",
            "-p",
            str(java_example),
            "bundle",
            "--no-daemon",
            "--console=plain",
        ]
        # gradlew runs from the upstream copy; only -p stays pointed at the local java_example.
        assert all(call.kwargs["cwd"] == upstream_java_sdk for call in mock_run.call_args_list)
        assert all("docker" not in call.args[0] for call in mock_run.call_args_list)
        assert (tmp_path / "java-artifacts" / "app.jar").exists()

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_container_mode_runs_in_docker(self, mock_run, tmp_path, java_example, upstream_java_sdk):
        _lang_sdk_build_java_jar(tmp_path, upstream_java_sdk, None, native=False)

        assert all(call.args[0][0] == "docker" for call in mock_run.call_args_list)
        cmd = mock_run.call_args_list[0].args[0]
        mounts = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-v"]
        # java-sdk is remounted to the upstream copy; GRADLE_USER_HOME lives outside it.
        assert f"{upstream_java_sdk}:/repo/java-sdk" in mounts
        assert "GRADLE_USER_HOME=/workspace-home/.gradle" in cmd
        assert any(mount.endswith(":/workspace-home/.gradle") for mount in mounts)


class TestLangSdkFetchUpstreamSdkSources:
    @mock.patch.object(kubernetes_commands, "run_command")
    def test_prefers_upstream_remote_when_present(self, mock_run, tmp_path):
        mock_run.side_effect = [
            mock.Mock(stdout="origin\nupstream\n"),
            mock.Mock(),  # git fetch
            mock.Mock(stdout="deadbeef\n"),  # git rev-parse
            mock.Mock(),  # git archive
            mock.Mock(),  # tar -xf
            mock.Mock(stdout=b""),  # git show gradlew
            mock.Mock(stdout=b""),  # git show gradlew.bat
            mock.Mock(stdout=b""),  # git show gradle-wrapper.jar
        ]

        _lang_sdk_fetch_upstream_sdk_sources(tmp_path, None)

        fetch_cmd = mock_run.call_args_list[1].args[0]
        assert fetch_cmd == ["git", "fetch", "--depth=1", "upstream", "main"]

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_falls_back_to_canonical_url_when_no_upstream_remote(self, mock_run, tmp_path):
        mock_run.side_effect = [
            mock.Mock(stdout="origin\n"),
            mock.Mock(),  # git fetch
            mock.Mock(stdout="deadbeef\n"),  # git rev-parse
            mock.Mock(),  # git archive
            mock.Mock(),  # tar -xf
            mock.Mock(stdout=b""),  # git show gradlew
            mock.Mock(stdout=b""),  # git show gradlew.bat
            mock.Mock(stdout=b""),  # git show gradle-wrapper.jar
        ]

        _lang_sdk_fetch_upstream_sdk_sources(tmp_path, None)

        fetch_cmd = mock_run.call_args_list[1].args[0]
        assert fetch_cmd == [
            "git",
            "fetch",
            "--depth=1",
            "https://github.com/apache/airflow.git",
            "main",
        ]

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_returns_extracted_go_sdk_and_java_sdk_paths(self, mock_run, tmp_path):
        mock_run.side_effect = [
            mock.Mock(stdout="upstream\n"),
            mock.Mock(),
            mock.Mock(stdout="deadbeef\n"),
            mock.Mock(),
            mock.Mock(),
            mock.Mock(stdout=b""),
            mock.Mock(stdout=b""),
            mock.Mock(stdout=b""),
        ]

        go_sdk, java_sdk = _lang_sdk_fetch_upstream_sdk_sources(tmp_path, None)

        extracted = tmp_path / "upstream_lang_sdk_sources"
        assert go_sdk == extracted / "go-sdk"
        assert java_sdk == extracted / "java-sdk"
        archive_cmd = mock_run.call_args_list[3].args[0]
        assert archive_cmd[:2] == ["git", "archive"]
        assert archive_cmd[-2:] == ["go-sdk", "java-sdk"]
        assert "deadbeef" in archive_cmd

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_symlinks_real_task_sdk_alongside_the_extraction(self, mock_run, tmp_path, monkeypatch):
        # java-sdk's build reads a sibling ../task-sdk/.../schema.json; without the symlink a
        # native-mode build from the extraction fails.
        repo_root = tmp_path / "repo"
        (repo_root / "task-sdk").mkdir(parents=True)
        monkeypatch.setattr(kubernetes_commands, "AIRFLOW_ROOT_PATH", repo_root)
        staging = tmp_path / "staging"
        staging.mkdir()
        mock_run.side_effect = [
            mock.Mock(stdout="upstream\n"),
            mock.Mock(),
            mock.Mock(stdout="deadbeef\n"),
            mock.Mock(),
            mock.Mock(),
            mock.Mock(stdout=b""),
            mock.Mock(stdout=b""),
            mock.Mock(stdout=b""),
        ]

        _lang_sdk_fetch_upstream_sdk_sources(staging, None)

        task_sdk_link = staging / "upstream_lang_sdk_sources" / "task-sdk"
        assert task_sdk_link.is_symlink()
        assert task_sdk_link.resolve() == (repo_root / "task-sdk").resolve()

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_restores_gradle_wrapper_files_dropped_by_export_ignore(self, mock_run, tmp_path):
        # export-ignore (ASF LEGAL-570) drops gradlew, gradlew.bat, and the wrapper jar from
        # `git archive`; the build invokes ./gradlew from the extraction, so all must be restored.
        mock_run.side_effect = [
            mock.Mock(stdout="upstream\n"),
            mock.Mock(),
            mock.Mock(stdout="deadbeef\n"),
            mock.Mock(),
            mock.Mock(),
            mock.Mock(stdout=b"fake-gradlew"),
            mock.Mock(stdout=b"fake-gradlew-bat"),
            mock.Mock(stdout=b"fake-jar-bytes"),
        ]

        _, java_sdk = _lang_sdk_fetch_upstream_sdk_sources(tmp_path, None)

        gradlew = java_sdk / "gradlew"
        assert gradlew.read_bytes() == b"fake-gradlew"
        assert gradlew.stat().st_mode & 0o111, "gradlew must be executable"
        assert (java_sdk / "gradlew.bat").read_bytes() == b"fake-gradlew-bat"
        assert (java_sdk / "gradle" / "wrapper" / "gradle-wrapper.jar").read_bytes() == b"fake-jar-bytes"
        show_cmds = [call.args[0] for call in mock_run.call_args_list[5:8]]
        assert show_cmds == [
            ["git", "show", "deadbeef:java-sdk/gradlew"],
            ["git", "show", "deadbeef:java-sdk/gradlew.bat"],
            ["git", "show", "deadbeef:java-sdk/gradle/wrapper/gradle-wrapper.jar"],
        ]


class TestLangSdkDryRun:
    """Dry-run skips the commands, so the filesystem work depending on their outputs must not run."""

    def test_fetch_upstream_sdk_sources_does_not_crash_on_str_stdout(self, dry_run, tmp_path, monkeypatch):
        monkeypatch.setattr(kubernetes_commands, "AIRFLOW_ROOT_PATH", tmp_path / "repo")

        _, java_sdk = _lang_sdk_fetch_upstream_sdk_sources(tmp_path, None)

        # Regression: dry-run run_command returns stdout="" (str) and write_bytes("") raised TypeError.
        assert (java_sdk / "gradlew").read_bytes() == b""

    def test_build_go_bundle_skips_copies_of_never_built_artifacts(self, dry_run, tmp_path, go_example):
        _lang_sdk_build_go_bundle(tmp_path, tmp_path / "missing_upstream_go_sdk", None, native=True)

        assert not (tmp_path / "go-artifacts" / kubernetes_commands.LANG_SDK_GO_BUNDLE_NAME).exists()

    def test_build_java_jar_skips_jar_copy(self, dry_run, tmp_path, java_example, upstream_java_sdk):
        (java_example / "build" / "bundle" / "app.jar").unlink()

        _lang_sdk_build_java_jar(tmp_path, upstream_java_sdk, None, native=True)

        assert not (tmp_path / "java-artifacts" / "app.jar").exists()

    @mock.patch.object(kubernetes_commands, "run_command_with_k8s_env")
    def test_upload_artifacts_uses_placeholder_for_never_built_jar(self, mock_run, dry_run, tmp_path):
        mock_run.return_value = mock.Mock(stdout="")

        _lang_sdk_upload_artifacts(tmp_path, "3.10", "v1.35.0", None)

        cp_sources = [call.args[0][2] for call in mock_run.call_args_list if call.args[0][1] == "cp"]
        assert str(tmp_path / "java-artifacts" / "app.jar") in cp_sources


class TestSetupLangSdkTestNativeSelection:
    @pytest.mark.parametrize(
        ("env_value", "expected_native"),
        [("true", True), ("True", True), ("false", False), ("", False), (None, False)],
    )
    def test_native_flag_is_read_from_env(self, monkeypatch, tmp_path, env_value, expected_native):
        if env_value is None:
            monkeypatch.delenv("LANG_SDK_NATIVE_TOOLCHAIN", raising=False)
        else:
            monkeypatch.setenv("LANG_SDK_NATIVE_TOOLCHAIN", env_value)

        captured: dict[str, bool] = {}
        fake_go_sdk = tmp_path / "upstream_go_sdk"
        fake_java_sdk = tmp_path / "upstream_java_sdk"

        def fake_parallel(steps, output):
            for _title, thunk in steps:
                thunk(None)

        monkeypatch.setattr(kubernetes_commands, "_run_lang_sdk_parallel", fake_parallel)
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_fetch_upstream_sdk_sources",
            lambda staging, output: (fake_go_sdk, fake_java_sdk),
        )
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_build_go_bundle",
            lambda staging, upstream_go_sdk, output, *, native: captured.update(
                go=native, go_sdk=upstream_go_sdk
            ),
        )
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_build_java_jar",
            lambda staging, upstream_java_sdk, output, *, native: captured.update(
                java=native, java_sdk=upstream_java_sdk
            ),
        )
        for name in (
            "_lang_sdk_deploy_localstack",
            "_lang_sdk_build_java_worker_image",
            "_lang_sdk_upload_artifacts",
            "_lang_sdk_apply_configmaps_and_secret",
            "_lang_sdk_deploy_airflow",
        ):
            monkeypatch.setattr(kubernetes_commands, name, lambda *a, **k: None)
        monkeypatch.setattr(
            kubernetes_commands,
            "BuildProdParams",
            lambda python: mock.Mock(airflow_image_kubernetes="img"),
        )

        kubernetes_commands._setup_lang_sdk_test(python="3.10", kubernetes_version="v1.35.0")

        assert captured == {
            "go": expected_native,
            "java": expected_native,
            "go_sdk": fake_go_sdk,
            "java_sdk": fake_java_sdk,
        }
