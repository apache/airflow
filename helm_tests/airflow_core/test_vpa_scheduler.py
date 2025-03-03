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


class TestSchedulerVPA:
    """Tests VPA."""

    def test_VPA_disabled_by_default(self):
        """Disabled by default."""
        docs = render_chart(
            values={},
            show_only=["templates/scheduler/scheduler-vpa.yaml"],
        )
        assert docs == []

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "vpa": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/scheduler/scheduler-vpa.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    @pytest.mark.parametrize(
        "update_mode",
        [
            ("Off"),
            ("Auto"),
        ],
    )
    def test_update_policy(self, update_mode):
        """Verify update policy."""
        docs = render_chart(
            values={
                "scheduler": {
                    "vpa": {
                        "enabled": True,
                        **({"updateMode": update_mode}),
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-vpa.yaml"],
        )
        assert jmespath.search("spec.updatePolicy.updateMode", docs[0]) == update_mode

    @pytest.mark.parametrize(
        "resource_policy, expected_resource_policy",
        [
            # custom resource policy
            (
                [
                    {
                        "minAllowed": {
                            "cpu": "250m",
                            "memory": "128Mi",
                        },
                        "maxAllowed": {
                            "cpu": "500m",
                            "memory": "2Gi",
                        },
                    }
                ],
                {
                        "containerName": "scheduler",
                        "minAllowed": {
                            "cpu": "250m",
                            "memory": "128Mi",
                        },
                        "maxAllowed": {
                            "cpu": "500m",
                            "memory": "2Gi",
                        },
                },
            ),
        ],
    )
    def test_should_use_vpa_resource_policy(self, resource_policy, expected_resource_policy):
        docs = render_chart(
            values={
                "scheduler": {
                    "vpa": {"enabled": True, **({"resourcePolicy": resource_policy})},
                },
            },
            show_only=["templates/scheduler/scheduler-vpa.yaml"],
        )
        assert expected_resource_policy == jmespath.search("spec.resourcePolicy.containerPolicies[0]", docs[0])

    @pytest.mark.parametrize(
        "extra_containers, expected_extra_containers",
        [
            # custom resource policy
            (
                [
                    {

                    "sync":
                        {
                            "minAllowed": {
                                "cpu": "250m",
                                "memory": "128Mi",
                            },
                            "maxAllowed": {
                                "cpu": "500m",
                                "memory": "2Gi",
                            },
                        }
                    }
                ],
                {
                    {
                    "sync":{
                            "minAllowed": {
                                "cpu": "250m",
                                "memory": "128Mi",
                            },
                            "maxAllowed": {
                                "cpu": "500m",
                                "memory": "2Gi",
                            },

                        }
                    }
                },
            ),
        ],
    )
    def test_should_use_vpa_extra_containers(self, extra_containers, expected_extra_containers):
        docs = render_chart(
            values={
                "scheduler": {
                    "vpa": {"enabled": True, **({"extraContainers": extra_containers})},
                },
            },
            show_only=["templates/scheduler/scheduler-vpa.yaml"],
        )
        assert expected_extra_containers == jmespath.search("spec.resourcePolicy.containerPolicies[1]", docs[0])
