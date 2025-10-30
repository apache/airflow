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
from chart_utils.helm_template_generator import render_chart


class TestDagProcessorDeployment:
    """Tests dag-processor deployment labels."""

    AIRFLOW_VERSION = "3.0.0"
    TEMPLATE_FILE = "templates/dag-processor/dag-processor-deployment.yaml"

    def test_should_add_global_labels(self):
        """Test adding only .Values.labels."""
        docs = render_chart(
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "labels": {"test_global_label": "test_global_label_value"},
            },
            show_only=[self.TEMPLATE_FILE],
        )

        assert "test_global_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_global_label"]
            == "test_global_label_value"
        )

    def test_should_add_component_specific_labels(self):
        """Test adding only .Values.dagProcessor.labels."""
        docs = render_chart(
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "dagProcessor": {
                    "labels": {"test_component_label": "test_component_label_value"},
                },
            },
            show_only=[self.TEMPLATE_FILE],
        )

        assert "test_component_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_component_label"]
            == "test_component_label_value"
        )

    def test_should_merge_global_and_component_specific_labels(self):
        """Test adding both .Values.labels and .Values.dagProcessor.labels."""
        docs = render_chart(
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "labels": {"test_global_label": "test_global_label_value"},
                "dagProcessor": {
                    "labels": {"test_component_label": "test_component_label_value"},
                },
            },
            show_only=[self.TEMPLATE_FILE],
        )

        assert "test_global_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_global_label"]
            == "test_global_label_value"
        )
        assert "test_component_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_component_label"]
            == "test_component_label_value"
        )

    def test_component_specific_labels_should_override_global_labels(self):
        """Test that component-specific labels take precedence over global labels with the same key."""
        docs = render_chart(
            values={
                "airflowVersion": self.AIRFLOW_VERSION,
                "labels": {"common_label": "global_value"},
                "dagProcessor": {
                    "labels": {"common_label": "component_value"},
                },
            },
            show_only=[self.TEMPLATE_FILE],
        )

        assert "common_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["common_label"]
            == "component_value"
        )

