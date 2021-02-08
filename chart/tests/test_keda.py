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
import unittest

import jmespath
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class KedaTest(unittest.TestCase):
    def test_keda_disabled_by_default(self):
        """disabled by default"""
        docs = render_chart(
            values={},
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
            validate_schema=False,
        )
        assert docs == []

    @parameterized.expand(
        [
            ('SequentialExecutor', False),
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
            validate_schema=False,
        )
        if is_created:
            assert "RELEASE-NAME-worker" == jmespath.search("metadata.name", docs[0])
        else:
            assert docs == []

    @parameterized.expand(
        [
            ("CeleryExecutor", 8),
            ("CeleryExecutor", 16),
            ("CeleryKubernetesExecutor", 8),
            ("CeleryKubernetesExecutor", 16),
        ]
    )
    def test_keda_concurrency(self, executor, concurrency):
        """
        ScaledObject should only be created when set to enabled and executor is Celery or CeleryKubernetes
        """
        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}, "persistence": {"enabled": False}},
                "executor": executor,
                "config": {"celery": {"worker_concurrency": concurrency}},
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
            validate_schema=False,
        )
        expected_query = (
            f"SELECT ceil(COUNT(*)::decimal / {concurrency}) "
            "FROM task_instance WHERE state='running' OR state='queued'"
        )
        self.assertEqual(expected_query, jmespath.search("spec.triggers[0].metadata.query", docs[0]))
