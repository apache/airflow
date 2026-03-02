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


class TestAPIServerHPA:
    """Tests HPA."""

    def test_hpa_disabled_by_default(self):
        """Disabled by default."""
        docs = render_chart(
            values={},
            show_only=["templates/api-server/api-server-hpa.yaml"],
        )
        assert docs == []

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.2",
                "apiServer": {
                    "hpa": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/api-server/api-server-hpa.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    @pytest.mark.parametrize(
        ("min_replicas", "max_replicas"),
        [
            (None, None),
            (2, 8),
        ],
    )
    def test_min_max_replicas(self, min_replicas, max_replicas):
        """Verify minimum and maximum replicas."""
        docs = render_chart(
            values={
                "airflowVersion": "3.0.2",
                "apiServer": {
                    "hpa": {
                        "enabled": True,
                        **({"minReplicaCount": min_replicas} if min_replicas else {}),
                        **({"maxReplicaCount": max_replicas} if max_replicas else {}),
                    }
                },
            },
            show_only=["templates/api-server/api-server-hpa.yaml"],
        )
        assert jmespath.search("spec.minReplicas", docs[0]) == 1 if min_replicas is None else min_replicas
        assert jmespath.search("spec.maxReplicas", docs[0]) == 5 if max_replicas is None else max_replicas

    def test_hpa_behavior(self):
        """Verify HPA behavior."""
        expected_behavior = {
            "scaleDown": {
                "stabilizationWindowSeconds": 300,
                "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
            }
        }
        docs = render_chart(
            values={
                "airflowVersion": "3.0.2",
                "apiServer": {
                    "hpa": {
                        "enabled": True,
                        "behavior": expected_behavior,
                    },
                },
            },
            show_only=["templates/api-server/api-server-hpa.yaml"],
        )
        assert jmespath.search("spec.behavior", docs[0]) == expected_behavior

    @pytest.mark.parametrize(
        ("metrics", "expected_metrics"),
        [
            # default metrics
            (
                None,
                {
                    "type": "Resource",
                    "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 50}},
                },
            ),
            # custom metric
            (
                [
                    {
                        "type": "Pods",
                        "pods": {
                            "metric": {"name": "custom"},
                            "target": {"type": "Utilization", "averageUtilization": 50},
                        },
                    }
                ],
                {
                    "type": "Pods",
                    "pods": {
                        "metric": {"name": "custom"},
                        "target": {"type": "Utilization", "averageUtilization": 50},
                    },
                },
            ),
        ],
    )
    def test_should_use_hpa_metrics(self, metrics, expected_metrics):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.2",
                "apiServer": {
                    "hpa": {"enabled": True, **({"metrics": metrics} if metrics else {})},
                },
            },
            show_only=["templates/api-server/api-server-hpa.yaml"],
        )
        assert jmespath.search("spec.metrics[0]", docs[0]) == expected_metrics
