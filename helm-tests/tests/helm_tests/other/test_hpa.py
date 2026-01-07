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
    def test_hpa_enabled(self, executor):
        """HPA should only be created when enabled and executor is Celery or CeleryKubernetes."""
        docs = render_chart(
            values={
                "workers": {"hpa": {"enabled": True}, "persistence": {"enabled": False}},
                "executor": executor,
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert jmespath.search("metadata.name", docs[0]) == "release-name-worker"

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
                "workers": {
                    "hpa": {
                        "enabled": True,
                        **({"minReplicaCount": min_replicas} if min_replicas else {}),
                        **({"maxReplicaCount": max_replicas} if max_replicas else {}),
                    }
                },
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert jmespath.search("spec.minReplicas", docs[0]) == 0 if min_replicas is None else min_replicas
        assert jmespath.search("spec.maxReplicas", docs[0]) == 5 if max_replicas is None else max_replicas

    @pytest.mark.parametrize(
        "executor", ["CeleryExecutor", "CeleryKubernetesExecutor", "CeleryExecutor,KubernetesExecutor"]
    )
    def test_hpa_behavior(self, executor):
        """Verify HPA behavior."""
        expected_behavior = {
            "scaleDown": {
                "stabilizationWindowSeconds": 300,
                "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
            }
        }
        docs = render_chart(
            values={
                "workers": {
                    "hpa": {
                        "enabled": True,
                        "behavior": expected_behavior,
                    },
                },
                "executor": executor,
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert jmespath.search("spec.behavior", docs[0]) == expected_behavior

    @pytest.mark.parametrize(
        ("enabled", "kind"),
        [
            (True, "StatefulSet"),
            (False, "Deployment"),
        ],
    )
    def test_persistence(self, enabled, kind):
        """If worker persistence is enabled, scaleTargetRef should be StatefulSet else Deployment."""
        docs = render_chart(
            values={
                "workers": {"hpa": {"enabled": True}, "persistence": {"enabled": enabled}},
                "executor": "CeleryExecutor",
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert jmespath.search("spec.scaleTargetRef.kind", docs[0]) == kind
