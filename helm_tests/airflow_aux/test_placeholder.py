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

from tests.charts.helm_template_generator import render_chart


class TestPlaceHolder:

    def test_priority_class_name(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {"placeHolder": {"enabled": True}},
            },
            show_only=[
                "templates/workers/placeholder/placeholder-deployment.yaml",
                "templates/workers/placeholder/placeholder-poddisruptionbudget.yaml",
                "templates/workers/placeholder/placeholder-priorityclass.yaml",
            ],
        )
        assert 3 == len(docs)
        assert "Deployment" == jmespath.search("kind", docs[0])
        assert "release-name-place-holder" == jmespath.search("metadata.name", docs[0])
        assert 2 == jmespath.search("spec.replicas", docs[0])
        assert "placeholder" == jmespath.search("spec.template.spec.containers[0].name", docs[0])
        assert "release-name-placeholder-pc" == jmespath.search("spec.template.spec.priorityClassName", docs[0])
        assert "PriorityClass" == jmespath.search("kind", docs[1])
        assert "release-name-placeholder-pc" == jmespath.search("metadata.name", docs[1])
        assert "PodDisruptionBudget" == jmespath.search("kind", docs[2])
        assert "release-name-placeholder-pdb" == jmespath.search("metadata.name", docs[2])
