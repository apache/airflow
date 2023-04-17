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

from tests.charts.helm_template_generator import render_chart


class TestCeleryKubernetesExecutor:
    def test_should_create_a_worker_deployment_with_the_celery_executor(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "dags": {"persistence": {"enabled": True}, "gitSync": {"enabled": True}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "config" == jmespath.search("spec.template.spec.volumes[0].name", docs[0])
        assert "dags" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])

    def test_should_create_a_worker_deployment_with_the_celery_kubernetes_executor(self):
        docs = render_chart(
            values={
                "executor": "CeleryKubernetesExecutor",
                "dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": False}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "config" == jmespath.search("spec.template.spec.volumes[0].name", docs[0])
        assert "dags" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])
