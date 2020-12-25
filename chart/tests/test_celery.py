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

from itertools import product

import jmespath
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class TestCelery:
    @parameterized.expand(
        product(['workers/worker', 'flower/flower'], ['CeleryExecutor', 'CeleryKubernetesExecutor'])
    )
    def test_must_use_celery_executor(self, deployment, executor):
        """
        When cluster is using CeleryKubernetesExecutor, the worker and flower must still use CeleryExecutor.
        To accomplish this we inject environment variable.
        """
        docs = render_chart(
            values={
                "executor": executor,
            },
            show_only=[f"templates/{deployment}-deployment.yaml"],
        )
        query = "spec.template.spec.containers[0].env[?name=='AIRFLOW__CORE__EXECUTOR'].value"
        assert jmespath.search(query, docs[0]) == ['CeleryExecutor']
