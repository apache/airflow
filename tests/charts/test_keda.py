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
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart


class TestKeda:
    def test_keda_disabled_by_default(self):
        """disabled by default"""
        docs = render_chart(
            values={},
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert docs == []

    @parameterized.expand(
        [
            ('CeleryExecutor', True),
            ('CeleryKubernetesExecutor', True),
        ]
    )
    def test_keda_enabled(self, executor, is_created):
        """
        ScaledObject should only be created when set to enabled and executor is Celery or CeleryKubernetes
        """
        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}, "persistence": {"enabled": False}},
                'executor': executor,
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        if is_created:
            assert jmespath.search("metadata.name", docs[0]) == "release-name-worker"
        else:
            assert docs == []

    @parameterized.expand(
        [
            ('CeleryExecutor'),
            ('CeleryKubernetesExecutor'),
        ]
    )
    def test_keda_advanced(self, executor):
        """
        Verify keda advanced config.
        """
        expected_advanced = {
            "horizontalPodAutoscalerConfig": {
                "behavior": {
                    "scaleDown": {
                        "stabilizationWindowSeconds": 300,
                        "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
                    }
                }
            }
        }
        docs = render_chart(
            values={
                "workers": {
                    "keda": {
                        "enabled": True,
                        "advanced": expected_advanced,
                    },
                },
                "executor": executor,
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert jmespath.search("spec.advanced", docs[0]) == expected_advanced

    @staticmethod
    def build_query(executor, concurrency=16, queue=None):
        """Builds the query used by KEDA autoscaler to determine how many workers there should be"""
        query = (
            f"SELECT ceil(COUNT(*)::decimal / {concurrency}) "
            "FROM task_instance WHERE (state='running' OR state='queued')"
        )
        if executor == 'CeleryKubernetesExecutor':
            query += f" AND queue != '{queue or 'kubernetes'}'"
        return query

    @pytest.mark.parametrize(
        'executor,concurrency',
        [
            ("CeleryExecutor", 8),
            ("CeleryExecutor", 16),
            ("CeleryKubernetesExecutor", 8),
            ("CeleryKubernetesExecutor", 16),
        ],
    )
    def test_keda_concurrency(self, executor, concurrency):
        """
        Verify keda sql query uses configured concurrency
        """
        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}, "persistence": {"enabled": False}},
                "executor": executor,
                "config": {"celery": {"worker_concurrency": concurrency}},
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        expected_query = self.build_query(executor=executor, concurrency=concurrency)
        assert jmespath.search("spec.triggers[0].metadata.query", docs[0]) == expected_query

    @pytest.mark.parametrize(
        'executor,queue,should_filter',
        [
            ("CeleryExecutor", None, False),
            ("CeleryExecutor", 'my_queue', False),
            ("CeleryKubernetesExecutor", None, True),
            ("CeleryKubernetesExecutor", 'my_queue', True),
        ],
    )
    def test_keda_query_kubernetes_queue(self, executor, queue, should_filter):
        """
        Verify keda sql query ignores kubernetes queue when CKE is used.
        Sometimes a user might want to use a different queue name for k8s executor tasks,
        and we also verify here that we use the configured queue name in that case.
        """
        values = {
            "workers": {"keda": {"enabled": True}, "persistence": {"enabled": False}},
            "executor": executor,
        }
        if queue:
            values.update({'config': {'celery_kubernetes_executor': {'kubernetes_queue': queue}}})
        docs = render_chart(
            values=values,
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        expected_query = self.build_query(executor=executor, queue=queue)
        assert jmespath.search("spec.triggers[0].metadata.query", docs[0]) == expected_query

    @parameterized.expand(
        [
            ('enabled', 'StatefulSet'),
            ('not_enabled', 'Deployment'),
        ]
    )
    def test_persistence(self, enabled, kind):
        """
        If worker persistence is enabled, scaleTargetRef should be StatefulSet else Deployment.
        """
        is_enabled = enabled == 'enabled'
        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}, "persistence": {"enabled": is_enabled}},
                'executor': 'CeleryExecutor',
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert jmespath.search("spec.scaleTargetRef.kind", docs[0]) == kind
