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


class TestHPA:
    """Tests HPA."""

    def test_hpa_disabled_by_default(self):
        """Disabled by default."""
        docs = render_chart(
            values={},
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert docs == []

    @pytest.mark.parametrize(
        "executor",
        [
            "CeleryExecutor",
            "CeleryKubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
        ],
    )
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"hpa": {"enabled": True}, "persistence": {"enabled": False}},
            {"celery": {"hpa": {"enabled": True}, "persistence": {"enabled": False}}},
        ],
    )
    def test_hpa_enabled(self, executor, workers_values):
        """HPA should only be created when enabled and executor is Celery or CeleryKubernetes."""
        docs = render_chart(
            values={
                "workers": workers_values,
                "executor": executor,
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert jmespath.search("metadata.name", docs[0]) == "release-name-worker"

    @pytest.mark.parametrize(
        "workers_values, expected_min, expected_max",
        [
            ({"hpa": {"enabled": True}}, 0, 5),
            ({"celery": {"hpa": {"enabled": True}}}, 0, 5),
            ({"hpa": {"enabled": True, "minReplicaCount": 2, "maxReplicaCount": 8}}, 2, 8),
            ({"celery": {"hpa": {"enabled": True, "minReplicaCount": 2, "maxReplicaCount": 8}}}, 2, 8),
        ],
    )
    def test_min_max_replicas(self, workers_values, expected_min, expected_max):
        """Verify minimum and maximum replicas."""
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert jmespath.search("spec.minReplicas", docs[0]) == expected_min
        assert jmespath.search("spec.maxReplicas", docs[0]) == expected_max

    @pytest.mark.parametrize(
        "executor", ["CeleryExecutor", "CeleryKubernetesExecutor", "CeleryExecutor,KubernetesExecutor"]
    )
    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "hpa": {
                    "enabled": True,
                    "behavior": {
                        "scaleDown": {
                            "stabilizationWindowSeconds": 300,
                            "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
                        }
                    },
                }
            },
            {
                "celery": {
                    "hpa": {
                        "enabled": True,
                        "behavior": {
                            "scaleDown": {
                                "stabilizationWindowSeconds": 300,
                                "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
                            }
                        },
                    }
                }
            },
            {
                "hpa": {
                    "enabled": True,
                    "behavior": {
                        "scaleUp": {
                            "stabilizationWindowSeconds": 30,
                            "policies": [{"type": "Percent", "value": 10, "periodSeconds": 5}],
                        }
                    },
                },
                "celery": {
                    "hpa": {
                        "enabled": True,
                        "behavior": {
                            "scaleDown": {
                                "stabilizationWindowSeconds": 300,
                                "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
                            }
                        },
                    }
                },
            },
        ],
    )
    def test_hpa_behavior(self, executor, workers_values):
        """Verify HPA behavior."""
        docs = render_chart(
            values={
                "workers": workers_values,
                "executor": executor,
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert jmespath.search("spec.behavior", docs[0]) == {
            "scaleDown": {
                "stabilizationWindowSeconds": 300,
                "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
            }
        }

    @pytest.mark.parametrize(
        "workers_values, kind",
        [
            ({"hpa": {"enabled": True}}, "StatefulSet"),
            ({"celery": {"hpa": {"enabled": True}}}, "StatefulSet"),
            ({"hpa": {"enabled": True}, "persistence": {"enabled": False}}, "Deployment"),
            ({"celery": {"hpa": {"enabled": True}, "persistence": {"enabled": False}}}, "Deployment"),
        ],
    )
    def test_persistence(self, workers_values, kind):
        """If worker persistence is enabled, scaleTargetRef should be StatefulSet else Deployment."""
        docs = render_chart(
            values={
                "workers": workers_values,
                "executor": "CeleryExecutor",
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert jmespath.search("spec.scaleTargetRef.kind", docs[0]) == kind
