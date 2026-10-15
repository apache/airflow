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
from click.testing import CliRunner

from airflow_breeze.commands import kubernetes_commands
from airflow_breeze.commands.kubernetes_commands import (
    _lang_sdk_build_go_bundle,
    _lang_sdk_build_java_jars,
    _lang_sdk_build_ts_bundles,
    _lang_sdk_fetch_upstream_sdk_sources,
    _lang_sdk_resolve_sdk_sources,
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
def java_native_bundle(tmp_path, monkeypatch):
    """Point the reusable native Java Dag fixture at a tmp path with a pre-built bundle jar."""
    native_dir = tmp_path / "java-native-bundle"
    (native_dir / "build" / "bundle").mkdir(parents=True)
    (native_dir / "build" / "bundle" / "airflow-e2e-java-native-bundle-all.jar").write_text("jar")
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_JAVA_NATIVE_BUNDLE_PATH", native_dir)
    return native_dir


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
        assert (tmp_path / "go-task-handlers" / kubernetes_commands.LANG_SDK_GO_BUNDLE_NAME).exists()
        # The scratch copy is re-tidied against the upstream go-sdk before packing, in the same dir.
        tidy_call = mock_run.call_args_list[0]
        assert tidy_call.args[0] == ["go", "mod", "tidy"]
        assert tidy_call.kwargs["cwd"] == workspace_example

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
        # The scratch copy is re-tidied in the same container image before packing.
        tidy_cmd = mock_run.call_args_list[0].args[0]
        assert tidy_cmd[0] == "docker"
        assert kubernetes_commands.LANG_SDK_GO_BUILDER_IMAGE in tidy_cmd
        assert tidy_cmd[-3:] == ["go", "mod", "tidy"]


@pytest.fixture
def ts_example(tmp_path, monkeypatch):
    """Point the repo root at a tmp path with a pre-built native TypeScript example bundle."""
    monkeypatch.setattr(kubernetes_commands, "AIRFLOW_ROOT_PATH", tmp_path)
    ts_dir = tmp_path / "ts-sdk" / "example"
    (ts_dir / "dist").mkdir(parents=True)
    (ts_dir / "dist" / kubernetes_commands.LANG_SDK_TS_BUNDLE_NAME).write_text("bundle")
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_TS_EXAMPLE_PATH", ts_dir)
    return ts_dir


@pytest.fixture
def k8s_ts_example(tmp_path, monkeypatch):
    """Point the mixed-language TypeScript handlers dir at a tmp path with a pre-built bundle."""
    ts_dir = tmp_path / "kubernetes-tests" / "lang_sdk" / "ts_example"
    (ts_dir / "dist").mkdir(parents=True)
    (ts_dir / "dist" / kubernetes_commands.LANG_SDK_TS_BUNDLE_NAME).write_text("bundle")
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_K8S_TS_EXAMPLE_PATH", ts_dir)
    return ts_dir


class TestLangSdkBuildTsBundles:
    BUILD = (
        "pnpm install --frozen-lockfile && pnpm run build "
        "&& cd example && pnpm install && pnpm run build && cd .. "
        "&& cd ../kubernetes-tests/lang_sdk/ts_example && pnpm install --frozen-lockfile && pnpm run build"
    )

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_native_builds_sdk_then_both_examples_on_host(
        self, mock_run, tmp_path, ts_example, k8s_ts_example
    ):
        _lang_sdk_build_ts_bundles(tmp_path / "staging", None, native=True)

        mock_run.assert_called_once()
        assert mock_run.call_args.args[0] == ["sh", "-c", self.BUILD]
        assert mock_run.call_args.kwargs["cwd"] == tmp_path / "ts-sdk"
        staging = tmp_path / "staging"
        assert (staging / "lang-sdk-native-ts" / kubernetes_commands.LANG_SDK_TS_BUNDLE_NAME).exists()
        assert (staging / "ts-task-handlers" / kubernetes_commands.LANG_SDK_TS_BUNDLE_NAME).exists()

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_container_mode_enables_corepack_as_the_container_user(
        self, mock_run, tmp_path, ts_example, k8s_ts_example
    ):
        _lang_sdk_build_ts_bundles(tmp_path / "staging", None, native=False)

        cmd = mock_run.call_args.args[0]
        assert cmd[0] == "docker"
        assert kubernetes_commands.LANG_SDK_TS_BUILDER_IMAGE in cmd
        home = tmp_path / "files" / "pnpm-home"
        assert f"HOME={home}" in cmd
        assert home.is_dir()
        script = cmd[-1]
        assert 'corepack enable --install-directory "$HOME/bin"' in script
        assert script.endswith(self.BUILD)


class TestLangSdkBuildJavaJars:
    @mock.patch.object(kubernetes_commands, "run_command")
    def test_native_uses_host_gradle_toolchain(
        self, mock_run, tmp_path, java_example, java_native_bundle, upstream_java_sdk
    ):
        _lang_sdk_build_java_jars(tmp_path, upstream_java_sdk, None, native=True)

        publish_cmd, example_cmd, native_cmd = (call.args[0] for call in mock_run.call_args_list)
        assert publish_cmd == [
            "./gradlew",
            "publishToMavenLocal",
            "-PskipSigning=true",
            "--no-daemon",
            "--console=plain",
        ]
        assert example_cmd == [
            "./gradlew",
            "-p",
            str(java_example),
            "bundle",
            "--no-daemon",
            "--console=plain",
        ]
        assert native_cmd == [
            "./gradlew",
            "-p",
            str(java_native_bundle),
            "bundle",
            "--no-daemon",
            "--console=plain",
        ]
        # gradlew runs from the upstream copy; only -p stays pointed at the local projects.
        assert all(call.kwargs["cwd"] == upstream_java_sdk for call in mock_run.call_args_list)
        assert all("docker" not in call.args[0] for call in mock_run.call_args_list)
        assert (tmp_path / "java-task-handlers" / "app.jar").exists()
        assert (tmp_path / "lang-sdk-native-java" / "airflow-e2e-java-native-bundle-all.jar").exists()

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_container_mode_runs_in_docker(
        self, mock_run, tmp_path, java_example, java_native_bundle, upstream_java_sdk
    ):
        _lang_sdk_build_java_jars(tmp_path, upstream_java_sdk, None, native=False)

        assert len(mock_run.call_args_list) == 3
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

        assert not (tmp_path / "go-task-handlers" / kubernetes_commands.LANG_SDK_GO_BUNDLE_NAME).exists()

    def test_build_java_jars_skips_jar_copies(
        self, dry_run, tmp_path, java_example, java_native_bundle, upstream_java_sdk
    ):
        (java_example / "build" / "bundle" / "app.jar").unlink()
        (java_native_bundle / "build" / "bundle" / "airflow-e2e-java-native-bundle-all.jar").unlink()

        _lang_sdk_build_java_jars(tmp_path, upstream_java_sdk, None, native=True)

        assert not (tmp_path / "java-task-handlers" / "app.jar").exists()
        assert not (tmp_path / "lang-sdk-native-java" / "airflow-e2e-java-native-bundle-all.jar").exists()

    @mock.patch.object(kubernetes_commands, "run_command_with_k8s_env")
    def test_upload_artifacts_uses_placeholder_for_never_built_jars(self, mock_run, dry_run, tmp_path):
        mock_run.return_value = mock.Mock(stdout="")

        _lang_sdk_upload_artifacts(tmp_path, "3.11", "v1.35.0", None)

        cp_sources = [call.args[0][2] for call in mock_run.call_args_list if call.args[0][1] == "cp"]
        assert str(tmp_path / "java-task-handlers" / "app.jar") in cp_sources
        assert str(tmp_path / "lang-sdk-native-java" / "app.jar") in cp_sources


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
        fake_go_sdk = tmp_path / "resolved_go_sdk"
        fake_java_sdk = tmp_path / "resolved_java_sdk"

        def fake_parallel(steps, output):
            for _title, thunk in steps:
                thunk(None)

        monkeypatch.setattr(kubernetes_commands, "_run_lang_sdk_parallel", fake_parallel)
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_resolve_sdk_sources",
            lambda staging, output: (fake_go_sdk, fake_java_sdk),
        )
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_build_go_bundle",
            lambda staging, go_sdk_source, output, *, native: captured.update(
                go=native, go_sdk=go_sdk_source
            ),
        )
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_build_java_jars",
            lambda staging, java_sdk_source, output, *, native: captured.update(
                java=native, java_sdk=java_sdk_source
            ),
        )
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_build_ts_bundles",
            lambda staging, output, *, native: captured.update(ts=native),
        )
        for name in (
            "_lang_sdk_deploy_localstack",
            "_lang_sdk_build_runtime_image",
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

        kubernetes_commands._setup_lang_sdk_test(python="3.11", kubernetes_version="v1.35.0")

        assert captured == {
            "go": expected_native,
            "java": expected_native,
            "ts": expected_native,
            "go_sdk": fake_go_sdk,
            "java_sdk": fake_java_sdk,
        }


class TestSetupLangSdkTestSteps:
    """Every lang-SDK artifact builds unconditionally: Go, Java and TypeScript all run."""

    @pytest.fixture
    def recorded(self, monkeypatch, tmp_path):
        recorded: dict[str, object] = {}

        def fake_parallel(steps, output):
            recorded["steps"] = [title for title, _thunk in steps]

        monkeypatch.setattr(kubernetes_commands, "_run_lang_sdk_parallel", fake_parallel)
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_resolve_sdk_sources",
            lambda staging, output: (tmp_path / "go_sdk", tmp_path / "java_sdk"),
        )
        monkeypatch.setattr(kubernetes_commands, "_lang_sdk_upload_artifacts", lambda *a, **k: None)
        for name in ("_lang_sdk_apply_configmaps_and_secret", "_lang_sdk_deploy_airflow"):
            monkeypatch.setattr(kubernetes_commands, name, lambda *a, **k: None)
        monkeypatch.setattr(
            kubernetes_commands,
            "BuildProdParams",
            lambda python: mock.Mock(airflow_image_kubernetes="img"),
        )
        return recorded

    def test_builds_go_java_ts_and_the_runtime_image_by_default(self, recorded):
        kubernetes_commands._setup_lang_sdk_test(python="3.10", kubernetes_version="v1.35.0")

        assert recorded["steps"] == [
            "Build Go bundle",
            "Build Java jars",
            "Build TypeScript bundles",
            "Deploy localstack",
            "Build runtime image",
        ]

    def test_skips_the_runtime_image_build_when_an_override_is_given(self, recorded):
        kubernetes_commands._setup_lang_sdk_test(
            python="3.10", kubernetes_version="v1.35.0", runtime_image="my-runtime:1"
        )

        assert recorded["steps"] == [
            "Build Go bundle",
            "Build Java jars",
            "Build TypeScript bundles",
            "Deploy localstack",
        ]

    @mock.patch.object(kubernetes_commands, "make_sure_kubernetes_tools_are_installed")
    @mock.patch.object(kubernetes_commands, "sync_virtualenv")
    @mock.patch.object(kubernetes_commands, "_setup_lang_sdk_test")
    def test_command_passes_the_runtime_image_override(self, mock_setup, mock_sync, _mock_tools):
        mock_sync.return_value = mock.Mock(returncode=0)

        result = CliRunner().invoke(
            kubernetes_commands.setup_lang_sdk_test, ["--runtime-image", "my-runtime:1"]
        )

        assert result.exit_code == 0, result.output
        assert mock_setup.call_args.kwargs["runtime_image"] == "my-runtime:1"


class TestLangSdkResolveSdkSources:
    @pytest.fixture
    def repo_root(self, tmp_path, monkeypatch):
        root = tmp_path / "repo"
        root.mkdir()
        monkeypatch.setattr(kubernetes_commands, "AIRFLOW_ROOT_PATH", root)
        return root

    @pytest.mark.parametrize(
        "env",
        [
            pytest.param({}, id="no-branch-env"),
            # A release-branch run must build the branch's own SDKs: upstream main's speak a later
            # supervisor_schema_version than that branch's task-SDK supervisor knows.
            pytest.param(
                {"GITHUB_BASE_REF": "v3-3-test", "DEFAULT_BRANCH": "v3-3-test"},
                id="release-branch-env",
            ),
        ],
    )
    @mock.patch.object(kubernetes_commands, "_lang_sdk_fetch_upstream_sdk_sources")
    def test_checkout_with_both_sdks_builds_from_them(
        self, mock_fetch, env, repo_root, tmp_path, monkeypatch
    ):
        monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
        monkeypatch.delenv("DEFAULT_BRANCH", raising=False)
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        (repo_root / "go-sdk").mkdir()
        (repo_root / "java-sdk").mkdir()

        go_sdk, java_sdk = _lang_sdk_resolve_sdk_sources(tmp_path, None)

        assert (go_sdk, java_sdk) == (repo_root / "go-sdk", repo_root / "java-sdk")
        mock_fetch.assert_not_called()

    @pytest.mark.parametrize(
        "present",
        [
            pytest.param((), id="neither-sdk"),
            pytest.param(("go-sdk",), id="only-go-sdk"),
            pytest.param(("java-sdk",), id="only-java-sdk"),
        ],
    )
    @mock.patch.object(kubernetes_commands, "_lang_sdk_fetch_upstream_sdk_sources")
    def test_checkout_without_both_sdks_falls_back_to_upstream_main(
        self, mock_fetch, present, repo_root, tmp_path
    ):
        for name in present:
            (repo_root / name).mkdir()
        mock_fetch.return_value = (tmp_path / "go-sdk", tmp_path / "java-sdk")

        go_sdk, java_sdk = _lang_sdk_resolve_sdk_sources(tmp_path, None)

        mock_fetch.assert_called_once_with(tmp_path, None)
        assert (go_sdk, java_sdk) == (tmp_path / "go-sdk", tmp_path / "java-sdk")
