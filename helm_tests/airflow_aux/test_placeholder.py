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


class TestPlaceHolder:
    def test_priority_class(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {"placeHolder": {"enabled": True}},
            },
            show_only=[
                "templates/workers/placeholder/placeholder-priorityclass.yaml",
            ],
        )
        assert 1 == len(docs)
        assert "release-name-placeholder-pc" == jmespath.search("metadata.name", docs[0])
        assert "PriorityClass" == jmespath.search("kind", docs[0])
        assert not jmespath.search("globalDefault", docs[0])
        assert -100 == jmespath.search("value", docs[0])

    def test_stateful_set(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {"placeHolder": {"enabled": True}},
            },
            show_only=[
                "templates/workers/placeholder/placeholder-statefulset.yaml",
            ],
        )
        assert 1 == len(docs)
        assert "StatefulSet" == jmespath.search("kind", docs[0])
        assert "placeholder" == jmespath.search("spec.template.spec.containers[-1].name", docs[0])
        assert "release-name-placeholder-pc" == jmespath.search(
            "spec.template.spec.priorityClassName", docs[0]
        )

    def test_poddisruptionbudget(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {"placeHolder": {"enabled": True}},
            },
            show_only=[
                "templates/workers/placeholder/placeholder-poddisruptionbudget.yaml",
            ],
        )
        assert 1 == len(docs)

        assert "PodDisruptionBudget" == jmespath.search("kind", docs[0])
        assert "release-name-placeholder-pdb" == jmespath.search("metadata.name", docs[0])
        assert 0 == jmespath.search("spec.minAvailable", docs[0])
