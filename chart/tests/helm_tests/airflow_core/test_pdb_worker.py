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


class TestWorkerPdb:
    """Tests Worker PDB."""

    def test_pod_disruption_budget_enabled(self):
        docs = render_chart(
            values={"workers": {"celery": {"podDisruptionBudget": {"enabled": True}}}},
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert len(docs) == 1

    def test_pod_disruption_budget_name(self):
        docs = render_chart(
            values={"workers": {"celery": {"podDisruptionBudget": {"enabled": True}}}},
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert jmespath.search("metadata.name", docs[0]) == "release-name-worker-pdb"

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "podDisruptionBudget": {"enabled": True},
                        "labels": {"test_label": "test_label_value"},
                    }
                }
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        labels = jmespath.search("metadata.labels", docs[0])
        assert labels["test_label"] == "test_label_value"
        assert "key" not in labels

    def test_pod_disruption_budget_config_max_unavailable_default(self):
        docs = render_chart(
            values={"workers": {"celery": {"podDisruptionBudget": {"enabled": True}}}},
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert jmespath.search("spec.maxUnavailable", docs[0]) == 1
        assert jmespath.search("spec.minAvailable", docs[0]) is None

    def test_pod_disruption_budget_config_max_unavailable_overwrite(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {"podDisruptionBudget": {"enabled": True, "config": {"maxUnavailable": 2}}}
                }
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert jmespath.search("spec.maxUnavailable", docs[0]) == 2
        assert jmespath.search("spec.minAvailable", docs[0]) is None

    def test_pod_disruption_budget_config_min_available_set(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "podDisruptionBudget": {
                            "enabled": True,
                            "config": {"maxUnavailable": None, "minAvailable": 1},
                        }
                    }
                }
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert jmespath.search("spec.maxUnavailable", docs[0]) is None
        assert jmespath.search("spec.minAvailable", docs[0]) == 1
