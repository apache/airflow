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

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart


class TestMultipleDagProcessingDeployment:
    """Tests for multipleDagProcessing deployment feature."""

    AIRFLOW_VERSION = "3.0.0"
    DEPLOYMENT_TEMPLATE = "templates/dag-processor/dag-processor-deployment.yaml"

    def test_default_single_deployment_when_multiple_dag_processing_not_set(self):
        """When multipleDagProcessing is not set, a single deployment is created."""
        docs = render_chart(
            name="test",
            values={"airflowVersion": self.AIRFLOW_VERSION},
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        assert len(docs) == 1
        assert jmespath.search("metadata.name", docs[0]) == "test-dag-processor"

    def test_default_single_deployment_when_multiple_dag_processing_empty(self):
        """When multipleDagProcessing is an empty list, a single deployment is created."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {"multipleDagProcessing": []},
            },
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        assert len(docs) == 1
        assert jmespath.search("metadata.name", docs[0]) == "test-dag-processor"

    def test_creates_multiple_deployments(self):
        """When multipleDagProcessing has entries, one deployment per entry is created."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags"},
                    ],
                },
            },
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        assert len(docs) == 2
        assert jmespath.search("[*].metadata.name", docs) == [
            "test-dag-processor-git-dags",
            "test-dag-processor-local-dags",
        ]

    def test_dag_processor_name_label_on_all_levels(self):
        """Each deployment gets a dag-processor-name label on metadata, selector, and pod template."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags"},
                    ],
                },
            },
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        for doc, expected_name in zip(docs, ["git-dags", "local-dags"]):
            assert jmespath.search('metadata.labels."dag-processor-name"', doc) == expected_name
            assert jmespath.search('spec.selector.matchLabels."dag-processor-name"', doc) == expected_name
            assert jmespath.search('spec.template.metadata.labels."dag-processor-name"', doc) == expected_name

    def test_no_dag_processor_name_label_when_not_using_multiple(self):
        """When multipleDagProcessing is not set, no dag-processor-name label is added."""
        docs = render_chart(
            name="test",
            values={"airflowVersion": self.AIRFLOW_VERSION},
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search('metadata.labels."dag-processor-name"', docs[0]) is None
        assert jmespath.search('spec.selector.matchLabels."dag-processor-name"', docs[0]) is None
        assert jmespath.search('spec.template.metadata.labels."dag-processor-name"', docs[0]) is None

    def test_replicas_fallback_and_override(self):
        """Replicas falls back to dagProcessor.replicas when not set, and can be overridden per entry."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "replicas": 5,
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags", "replicas": 2},
                    ],
                },
            },
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.replicas", docs[0]) == 5
        assert jmespath.search("spec.replicas", docs[1]) == 2

    def test_args_fallback_and_override(self):
        """Args falls back to dagProcessor.args when not set, and can be overridden per entry."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "args": ["bash", "-c", "exec airflow dag-processor --subdir /opt/airflow/dags/default"],
                    "multipleDagProcessing": [
                        {"name": "default-args"},
                        {
                            "name": "custom-args",
                            "args": [
                                "bash",
                                "-c",
                                "exec airflow dag-processor --subdir /opt/airflow/dags/custom",
                            ],
                        },
                    ],
                },
            },
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search(
            "spec.template.spec.containers[?name=='dag-processor'] | [0].args", docs[0]
        ) == ["bash", "-c", "exec airflow dag-processor --subdir /opt/airflow/dags/default"]
        assert jmespath.search(
            "spec.template.spec.containers[?name=='dag-processor'] | [0].args", docs[1]
        ) == ["bash", "-c", "exec airflow dag-processor --subdir /opt/airflow/dags/custom"]

    def test_global_labels_applied_to_all_deployments(self):
        """Global labels are applied to all multiple dag processor deployments."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "labels": {"global_label": "global_value"},
                "dagProcessor": {
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags"},
                    ],
                },
            },
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        for doc in docs:
            pod_labels = jmespath.search("spec.template.metadata.labels", doc)
            assert pod_labels["global_label"] == "global_value"

    def test_component_labels_applied_to_all_deployments(self):
        """Component-specific labels are applied to all multiple dag processor deployments."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "labels": {"component_label": "component_value"},
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags"},
                    ],
                },
            },
            show_only=[self.DEPLOYMENT_TEMPLATE],
        )

        for doc in docs:
            pod_labels = jmespath.search("spec.template.metadata.labels", doc)
            assert pod_labels["component_label"] == "component_value"


class TestMultipleDagProcessingPodDisruptionBudget:
    """Tests for multipleDagProcessing PodDisruptionBudget feature."""

    AIRFLOW_VERSION = "3.0.0"
    PDB_TEMPLATE = "templates/dag-processor/dag-processor-poddisruptionbudget.yaml"

    def test_default_single_pdb_when_multiple_dag_processing_not_set(self):
        """When multipleDagProcessing is not set, a single PDB is created."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {"podDisruptionBudget": {"enabled": True}},
            },
            show_only=[self.PDB_TEMPLATE],
        )

        assert len(docs) == 1
        assert jmespath.search("metadata.name", docs[0]) == "test-dag-processor-pdb"

    def test_creates_multiple_pdbs(self):
        """When multipleDagProcessing has entries, one PDB per entry is created."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "podDisruptionBudget": {"enabled": True},
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags"},
                    ],
                },
            },
            show_only=[self.PDB_TEMPLATE],
        )

        assert len(docs) == 2
        assert jmespath.search("[*].metadata.name", docs) == [
            "test-dag-processor-git-dags-pdb",
            "test-dag-processor-local-dags-pdb",
        ]

    def test_pdb_dag_processor_name_label_and_selector(self):
        """Each PDB gets a dag-processor-name label and matching selector."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "podDisruptionBudget": {"enabled": True},
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags"},
                    ],
                },
            },
            show_only=[self.PDB_TEMPLATE],
        )

        for doc, expected_name in zip(docs, ["git-dags", "local-dags"]):
            assert jmespath.search('metadata.labels."dag-processor-name"', doc) == expected_name
            assert jmespath.search('spec.selector.matchLabels."dag-processor-name"', doc) == expected_name

    def test_no_pdb_dag_processor_name_label_when_not_using_multiple(self):
        """When multipleDagProcessing is not set, no dag-processor-name label on PDB."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {"podDisruptionBudget": {"enabled": True}},
            },
            show_only=[self.PDB_TEMPLATE],
        )

        assert jmespath.search('metadata.labels."dag-processor-name"', docs[0]) is None
        assert jmespath.search('spec.selector.matchLabels."dag-processor-name"', docs[0]) is None

    @pytest.mark.parametrize(
        "pdb_config",
        [
            {"maxUnavailable": 1},
            {"minAvailable": 1},
        ],
    )
    def test_pdb_config_applied_to_all_instances(self, pdb_config):
        """PDB config is applied to all multiple dag processor PDBs."""
        docs = render_chart(
            name="test",
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "podDisruptionBudget": {"enabled": True, "config": pdb_config},
                    "multipleDagProcessing": [
                        {"name": "git-dags"},
                        {"name": "local-dags"},
                    ],
                },
            },
            show_only=[self.PDB_TEMPLATE],
        )

        for doc in docs:
            for key, value in pdb_config.items():
                assert jmespath.search(f"spec.{key}", doc) == value
