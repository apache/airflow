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


class TestAPIServerRolloutRestartCronJob:
    """Tests API Server rollout restart CronJob."""

    def test_should_create_cronjob_when_enabled(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "enabled": True,
                    "rolloutRestart": {
                        "enabled": True,
                    },
                },
            },
            show_only=["templates/api-server/api-server-rollout-restart-cronjob.yaml"],
        )

        assert len(docs) == 1
        assert docs[0]["kind"] == "CronJob"
        assert docs[0]["metadata"]["name"] == "release-name-api-server-rollout-restart"

        containers = jmespath.search("spec.jobTemplate.spec.template.spec.containers", docs[0])
        assert len(containers) == 1
        assert containers[0]["name"] == "api-server-rollout-restart"
        assert containers[0]["image"] == "bitnami/kubectl:1.30.2"
        assert containers[0]["args"] == [
            "rollout",
            "restart",
            "deployment/release-name-api-server",
            "--namespace",
            "default",
        ]

    def test_should_not_create_cronjob_when_disabled(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "enabled": True,
                    "rolloutRestart": {
                        "enabled": False,
                    },
                },
            },
            show_only=["templates/api-server/api-server-rollout-restart-cronjob.yaml"],
        )

        assert len(docs) == 0

    def test_should_use_custom_deployment_name(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "enabled": True,
                    "rolloutRestart": {
                        "enabled": True,
                        "deploymentName": "my-custom-deployment-name",
                    },
                },
            },
            show_only=["templates/api-server/api-server-rollout-restart-cronjob.yaml"],
        )

        containers = jmespath.search("spec.jobTemplate.spec.template.spec.containers", docs[0])
        assert containers[0]["args"] == [
            "rollout",
            "restart",
            "deployment/my-custom-deployment-name",
            "--namespace",
            "default",
        ]
