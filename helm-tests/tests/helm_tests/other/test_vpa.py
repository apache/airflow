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


class TestVPA:
    """Tests Worker and Scheduler VPA."""

    def test_VPA_disabled_by_default(self):
        """Disabled by default."""
        docs = render_chart(
            values={},
            show_only=[
                "templates/scheduler/scheduler-vpa.yaml",
                "templates/workers/worker-vpa.yaml",
                "templates/webserver/webserver-vpa.yaml",
            ],
        )
        assert docs == []

    @pytest.mark.parametrize(
        "component",
        [
            ("scheduler"),
            ("worker"),
            ("webserver"),
        ],
    )
    def test_scheduler_should_add_component_specific_labels(self, component):
        root_component = component if component != "worker" else "workers"
        docs = render_chart(
            values={
                root_component: {
                    "vpa": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=[f"templates/{root_component}/{component}-vpa.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    @pytest.mark.parametrize(
        "update_mode, component",
        [
            ("Recreate", "webserver"),
            ("Auto", "webserver"),
            ("Recreate", "worker"),
            ("Auto", "worker"),
            ("Recreate", "scheduler"),
            ("Auto", "scheduler"),
        ],
    )
    def test_update_policy(self, update_mode, component):
        """Verify update policy."""
        root_component = component if component != "worker" else "workers"
        docs = render_chart(
            values={
                root_component: {
                    "vpa": {
                        "enabled": True,
                        "updateMode": update_mode,
                    }
                },
            },
            show_only=[f"templates/{root_component}/{component}-vpa.yaml"],
        )
        assert jmespath.search("spec.updatePolicy.updateMode", docs[0]) == update_mode

    @pytest.mark.parametrize(
        "resource_policy, expected_resource_policy, component",
        [
            # custom resource policy
            (
                {
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "2Gi",
                    },
                },
                {
                    "containerName": "webserver",
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "2Gi",
                    },
                },
                "webserver",
            ),
            (
                {
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "2Gi",
                    },
                },
                {
                    "containerName": "worker",
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "2Gi",
                    },
                },
                "worker",
            ),
            (
                {
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "2Gi",
                    },
                },
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
                "scheduler",
            ),
        ],
    )
    def test_should_use_vpa_resource_policy(self, resource_policy, expected_resource_policy, component):
        root_component = component if component != "worker" else "workers"
        docs = render_chart(
            values={
                root_component: {
                    "vpa": {"enabled": True, "resourcePolicy": resource_policy},
                },
            },
            show_only=[f"templates/{root_component}/{component}-vpa.yaml"],
        )
        assert expected_resource_policy == jmespath.search(
            "spec.resourcePolicy.containerPolicies[0]", docs[0]
        )

    @pytest.mark.parametrize(
        "extra_containers, expected_extra_containers, component",
        [
            # custom resource policy
            (
                [
                    {
                        "containerName": "sync",
                        "minAllowed": {
                            "cpu": "250m",
                            "memory": "128Mi",
                        },
                        "maxAllowed": {
                            "cpu": "500m",
                            "memory": "600Mi",
                        },
                    }
                ],
                {
                    "containerName": "sync",
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "600Mi",
                    },
                },
                "webserver",
            ),
            (
                [
                    {
                        "containerName": "sync",
                        "minAllowed": {
                            "cpu": "250m",
                            "memory": "128Mi",
                        },
                        "maxAllowed": {
                            "cpu": "500m",
                            "memory": "600Mi",
                        },
                    },
                ],
                {
                    "containerName": "sync",
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "600Mi",
                    },
                },
                "worker",
            ),
            (
                [
                    {
                        "containerName": "sync",
                        "minAllowed": {
                            "cpu": "250m",
                            "memory": "128Mi",
                        },
                        "maxAllowed": {
                            "cpu": "500m",
                            "memory": "600Mi",
                        },
                    },
                ],
                {
                    "containerName": "sync",
                    "minAllowed": {
                        "cpu": "250m",
                        "memory": "128Mi",
                    },
                    "maxAllowed": {
                        "cpu": "500m",
                        "memory": "600Mi",
                    },
                },
                "scheduler",
            ),
        ],
    )
    def test_should_use_vpa_extra_containers(self, extra_containers, expected_extra_containers, component):
        root_component = component if component != "worker" else "workers"
        docs = render_chart(
            values={
                root_component: {
                    "vpa": {"enabled": True, "extraContainers": extra_containers},
                },
            },
            show_only=[f"templates/{root_component}/{component}-vpa.yaml"],
        )
        assert expected_extra_containers == jmespath.search(
            "spec.resourcePolicy.containerPolicies[1]", docs[0]
        )
