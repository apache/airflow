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
)


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
def java_example(tmp_path, monkeypatch):
    """Point the java_example dir at a tmp path (under a tmp repo root) with a pre-built bundle jar."""
    monkeypatch.setattr(kubernetes_commands, "AIRFLOW_ROOT_PATH", tmp_path)
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_MAVEN_CACHE_PATH", tmp_path / "m2")
    java_dir = tmp_path / "java_example"
    (java_dir / "build" / "bundle").mkdir(parents=True)
    (java_dir / "build" / "bundle" / "app.jar").write_text("jar")
    monkeypatch.setattr(kubernetes_commands, "LANG_SDK_JAVA_EXAMPLE_PATH", java_dir)
    return java_dir


class TestLangSdkBuildGoBundle:
    @mock.patch.object(kubernetes_commands, "run_command")
    def test_native_uses_host_go_toolchain(self, mock_run, tmp_path, go_example):
        _lang_sdk_build_go_bundle(tmp_path, None, native=True)

        cmd = mock_run.call_args.args[0]
        assert cmd[:3] == ["go", "tool", "airflow-go-pack"]
        assert "docker" not in cmd
        assert mock_run.call_args.kwargs["cwd"] == go_example
        assert mock_run.call_args.kwargs["env"]["CGO_ENABLED"] == "0"
        assert (tmp_path / "go-artifacts" / kubernetes_commands.LANG_SDK_GO_BUNDLE_NAME).exists()

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_container_mode_runs_in_docker(self, mock_run, tmp_path, go_example):
        _lang_sdk_build_go_bundle(tmp_path, None, native=False)

        cmd = mock_run.call_args.args[0]
        assert cmd[0] == "docker"
        assert kubernetes_commands.LANG_SDK_GO_BUILDER_IMAGE in cmd


class TestLangSdkBuildJavaJar:
    @mock.patch.object(kubernetes_commands, "run_command")
    def test_native_uses_host_gradle_toolchain(self, mock_run, tmp_path, java_example):
        _lang_sdk_build_java_jar(tmp_path, None, native=True)

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
        assert all("docker" not in call.args[0] for call in mock_run.call_args_list)
        assert (tmp_path / "java-artifacts" / "app.jar").exists()

    @mock.patch.object(kubernetes_commands, "run_command")
    def test_container_mode_runs_in_docker(self, mock_run, tmp_path, java_example):
        _lang_sdk_build_java_jar(tmp_path, None, native=False)

        assert all(call.args[0][0] == "docker" for call in mock_run.call_args_list)


class TestSetupLangSdkTestNativeSelection:
    @pytest.mark.parametrize(
        ("env_value", "expected_native"),
        [("true", True), ("True", True), ("false", False), ("", False), (None, False)],
    )
    def test_native_flag_is_read_from_env(self, monkeypatch, env_value, expected_native):
        if env_value is None:
            monkeypatch.delenv("LANG_SDK_NATIVE_TOOLCHAIN", raising=False)
        else:
            monkeypatch.setenv("LANG_SDK_NATIVE_TOOLCHAIN", env_value)

        captured: dict[str, bool] = {}

        def fake_parallel(steps, output):
            for _title, thunk in steps:
                thunk(None)

        monkeypatch.setattr(kubernetes_commands, "_run_lang_sdk_parallel", fake_parallel)
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_build_go_bundle",
            lambda staging, output, *, native: captured.update(go=native),
        )
        monkeypatch.setattr(
            kubernetes_commands,
            "_lang_sdk_build_java_jar",
            lambda staging, output, *, native: captured.update(java=native),
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

        assert captured == {"go": expected_native, "java": expected_native}
