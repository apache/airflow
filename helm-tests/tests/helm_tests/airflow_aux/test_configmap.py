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
import re

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart


class TestConfigmap:
    """Tests configmaps."""

    def test_single_annotation(self):
        docs = render_chart(
            values={
                "airflowConfigAnnotations": {"key": "value"},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations.get("key") == "value"

    def test_multiple_annotations(self):
        docs = render_chart(
            values={
                "airflowConfigAnnotations": {"key": "value", "key-two": "value-two"},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations.get("key") == "value"
        assert annotations.get("key-two") == "value-two"

    @pytest.mark.parametrize(("af_version", "expected"), [("3.0.0", False), ("2.11.0", True)])
    def test_default_airflow_local_settings(self, af_version, expected):
        docs = render_chart(
            values={
                "airflowVersion": af_version,
                "webserverSecretKey": None,
                "webserverSecretKeySecretName": None,
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )
        if expected:
            assert (
                "Usage of a dynamic webserver secret key detected"
                in jmespath.search('data."airflow_local_settings.py"', docs[0]).strip()
            )
        else:
            assert jmespath.search('data."airflow_local_settings.py"', docs[0]).strip() == ""

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello {{ .Release.Name }}!"},
            show_only=["templates/configmaps/configmap.yaml"],
        )

        assert (
            jmespath.search('data."airflow_local_settings.py"', docs[0]).strip()
            == "# Well hello release-name!"
        )

    def test_kerberos_config_available_with_celery_executor(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "kerberos": {"enabled": True, "config": "krb5\ncontent"},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        assert jmespath.search('data."krb5.conf"', docs[0]) == "krb5\ncontent"

    @pytest.mark.parametrize(
        ("executor", "af_version", "should_be_created"),
        [
            ("KubernetesExecutor", "2.11.0", True),
            ("KubernetesExecutor", "3.0.0", True),
            ("CeleryExecutor", "2.11.0", False),
            ("CeleryExecutor", "3.0.0", False),
            ("CeleryExecutor,KubernetesExecutor", "2.11.0", True),
            ("CeleryExecutor,KubernetesExecutor", "3.0.0", True),
        ],
    )
    def test_pod_template_created(self, executor, af_version, should_be_created):
        docs = render_chart(
            values={
                "executor": executor,
                "airflowVersion": af_version,
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        keys = jmespath.search("data", docs[0]).keys()
        if should_be_created:
            assert "pod_template_file.yaml" in keys
        else:
            assert "pod_template_file.yaml" not in keys

    def test_pod_template_is_templated(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "podTemplate": """
apiVersion: v1
kind: Pod
metadata:
  name: example-name
  labels:
    mylabel: {{ .Release.Name }}
""",
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        pod_template_file = jmespath.search('data."pod_template_file.yaml"', docs[0])
        assert "mylabel: release-name" in pod_template_file

    def test_default_flower_url_prefix(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )
        expected = "flower_url_prefix = "
        cfg = jmespath.search('data."airflow.cfg"', docs[0])
        assert expected in cfg.splitlines()

    def test_overridedn_flower_url_prefix(self):
        docs = render_chart(
            values={"executor": "CeleryExecutor", "ingress": {"flower": {"path": "/overridden-path"}}},
            show_only=["templates/configmaps/configmap.yaml"],
        )

        expected = "flower_url_prefix = /overridden-path"

        cfg = jmespath.search('data."airflow.cfg"', docs[0])
        assert expected in cfg.splitlines()

    @pytest.mark.parametrize(
        ("dag_values", "expected_default_dag_folder"),
        [
            (
                {"gitSync": {"enabled": True}},
                "/opt/airflow/dags/repo/tests/dags",
            ),
            (
                {"persistence": {"enabled": True}},
                "/opt/airflow/dags",
            ),
            (
                {"mountPath": "/opt/airflow/dags/custom", "gitSync": {"enabled": True}},
                "/opt/airflow/dags/custom/repo/tests/dags",
            ),
            (
                {
                    "mountPath": "/opt/airflow/dags/custom",
                    "gitSync": {"enabled": True, "subPath": "mysubPath"},
                },
                "/opt/airflow/dags/custom/repo/mysubPath",
            ),
            (
                {"mountPath": "/opt/airflow/dags/custom", "persistence": {"enabled": True}},
                "/opt/airflow/dags/custom",
            ),
        ],
    )
    def test_expected_default_dag_folder(self, dag_values, expected_default_dag_folder):
        docs = render_chart(
            values={"dags": dag_values},
            show_only=["templates/configmaps/configmap.yaml"],
        )
        cfg = jmespath.search('data."airflow.cfg"', docs[0])
        expected_folder_config = f"dags_folder = {expected_default_dag_folder}"
        assert expected_folder_config in cfg.splitlines()

    @pytest.mark.parametrize(
        ("airflow_version", "enabled"),
        [
            ("2.11.0", False),
            ("3.0.0", True),
        ],
    )
    def test_default_standalone_dag_processor_by_airflow_version(self, airflow_version, enabled):
        docs = render_chart(
            values={"airflowVersion": airflow_version},
            show_only=["templates/configmaps/configmap.yaml"],
        )

        cfg = jmespath.search('data."airflow.cfg"', docs[0])
        expected_line = f"standalone_dag_processor = {enabled}"
        assert expected_line in cfg.splitlines()

    @pytest.mark.parametrize(
        ("airflow_version", "enabled"),
        [
            ("2.11.0", False),
            ("2.11.0", True),
            ("3.0.0", False),
            ("3.0.0", True),
        ],
    )
    def test_standalone_dag_processor_explicit(self, airflow_version, enabled):
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
                "config": {"scheduler": {"standalone_dag_processor": enabled}},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        cfg = jmespath.search('data."airflow.cfg"', docs[0])
        expected_line = f"standalone_dag_processor = {str(enabled).lower()}"
        assert expected_line in cfg.splitlines()

    @pytest.mark.parametrize(
        ("airflow_version", "base_url", "execution_api_server_url", "expected_execution_url"),
        [
            (
                "3.0.0",
                None,
                None,
                "http://release-name-api-server:8080/execution/",
            ),
            (
                "2.11.0",
                None,
                None,
                None,
            ),
            (
                "3.0.0",
                "http://example.com",
                None,
                "http://release-name-api-server:8080/execution/",
            ),
            (
                "3.0.0",
                "http://example.com/airflow",
                None,
                "http://release-name-api-server:8080/airflow/execution/",
            ),
            (
                "3.0.0",
                "http://example.com/airflow",
                "http://service:9090/execution/",
                "http://service:9090/execution/",
            ),
        ],
    )
    def test_execution_api_server_url(
        self, airflow_version, base_url, execution_api_server_url, expected_execution_url
    ):
        config_overrides = {}
        if base_url:
            config_overrides["api"] = {"base_url": base_url}

        if execution_api_server_url:
            config_overrides["core"] = {"execution_api_server_url": execution_api_server_url}

        configmap = render_chart(
            values={"airflowVersion": airflow_version, "config": config_overrides},
            show_only=["templates/configmaps/configmap.yaml"],
        )

        config = jmespath.search('data."airflow.cfg"', configmap[0])
        assert config is not None, "Configmap data for airflow.cfg should not be None"
        assert len(config) > 0, "Configmap data for airflow.cfg should not be empty"

        if expected_execution_url is not None:
            assert f"\nexecution_api_server_url = {expected_execution_url}\n" in config
        else:
            assert "execution_api_server_url" not in config, (
                "execution_api_server_url should not be set for Airflow 2.11 versions"
            )

    @pytest.mark.parametrize(
        ("git_sync_enabled", "ssh_key_secret", "expected_volume"),
        [
            (True, "my-secret", True),
            (True, None, False),
            (False, "my-secret", False),
        ],
    )
    def test_pod_template_git_sync_ssh_key_volume_with_ssh_key_secret_name(
        self, git_sync_enabled, ssh_key_secret, expected_volume
    ):
        dag_values = {"gitSync": {"enabled": git_sync_enabled, "sshKeySecret": ssh_key_secret}}
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "dags": dag_values,
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        pod_template_file = jmespath.search('data."pod_template_file.yaml"', docs[0])
        assert ("git-sync-ssh-key" in pod_template_file) == expected_volume

    @pytest.mark.parametrize(
        ("git_sync_enabled", "ssh_key", "expected_volume"),
        [
            (True, "my-key", True),
            (True, None, False),
            (False, "my-key", False),
        ],
    )
    def test_pod_template_git_sync_ssh_key_volume_with_ssh_key(
        self, git_sync_enabled, ssh_key, expected_volume
    ):
        dag_values = {"gitSync": {"enabled": git_sync_enabled, "sshKey": ssh_key}}
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "dags": dag_values,
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        pod_template_file = jmespath.search('data."pod_template_file.yaml"', docs[0])
        assert ("git-sync-ssh-key" in pod_template_file) == expected_volume

    @pytest.mark.parametrize(
        ("scheduler_cpu_limit", "expected_sync_parallelism"),
        [
            ("1m", "1"),
            ("1000m", "1"),
            ("1001m", "2"),
            ("0.1", "1"),
            ("1", "1"),
            ("1.01", "2"),
            (None, 0),
            (0, 0),
        ],
    )
    def test_expected_celery_sync_parallelism(self, scheduler_cpu_limit, expected_sync_parallelism):
        scheduler_resources_cpu_limit = {}
        if scheduler_cpu_limit is not None:
            scheduler_resources_cpu_limit = {
                "scheduler": {"resources": {"limits": {"cpu": scheduler_cpu_limit}}}
            }

        configmap = render_chart(
            values=scheduler_resources_cpu_limit,
            show_only=["templates/configmaps/configmap.yaml"],
        )
        config = jmespath.search('data."airflow.cfg"', configmap[0])
        assert f"\nsync_parallelism = {expected_sync_parallelism}\n" in config

    def test_dag_bundle_config_list(self):
        """Test dag_bundle_config_list is generated from dagProcessor.dagBundleConfigList."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                            "kwargs": {
                                "git_conn_id": "GITHUB__repo1",
                                "subdir": "dags",
                                "tracking_ref": "main",
                                "refresh_interval": 60,
                            },
                        },
                        {
                            "name": "bundle2",
                            "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                            "kwargs": {
                                "git_conn_id": "GITHUB__repo2",
                                "subdir": "dags",
                                "tracking_ref": "develop",
                                "refresh_interval": 120,
                            },
                        },
                        {
                            "name": "dags-folder",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                },
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        cfg = jmespath.search('data."airflow.cfg"', docs[0])
        assert "[dag_processor]" in cfg

        # Extract the JSON value from dag_bundle_config_list
        match = re.search(r"dag_bundle_config_list\s*=\s*(.+)", cfg)
        assert match is not None, "dag_bundle_config_list not found in config"
        json_str = match.group(1).strip()

        # Parse and validate JSON structure
        bundles = json.loads(json_str)
        assert isinstance(bundles, list), "dag_bundle_config_list should be a list"
        assert len(bundles) == 3, f"Expected 3 bundles, got {len(bundles)}"

        # Verify bundle1
        bundle1 = next((b for b in bundles if b["name"] == "bundle1"), None)
        assert bundle1 is not None, "bundle1 not found"
        assert bundle1["classpath"] == "airflow.providers.git.bundles.git.GitDagBundle"
        assert bundle1["kwargs"]["git_conn_id"] == "GITHUB__repo1"
        assert bundle1["kwargs"]["subdir"] == "dags"
        assert bundle1["kwargs"]["tracking_ref"] == "main"
        assert bundle1["kwargs"]["refresh_interval"] == 60

        # Verify bundle2
        bundle2 = next((b for b in bundles if b["name"] == "bundle2"), None)
        assert bundle2 is not None, "bundle2 not found"
        assert bundle2["classpath"] == "airflow.providers.git.bundles.git.GitDagBundle"
        assert bundle2["kwargs"]["git_conn_id"] == "GITHUB__repo2"
        assert bundle2["kwargs"]["subdir"] == "dags"
        assert bundle2["kwargs"]["tracking_ref"] == "develop"
        assert bundle2["kwargs"]["refresh_interval"] == 120

        # Verify dags-folder
        dags_folder = next((b for b in bundles if b["name"] == "dags-folder"), None)
        assert dags_folder is not None, "dags-folder not found"
        assert dags_folder["classpath"] == "airflow.dag_processing.bundles.local.LocalDagBundle"
        assert dags_folder["kwargs"] == {}
