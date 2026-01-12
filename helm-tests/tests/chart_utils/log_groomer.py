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


class LogGroomerTestBase:
    obj_name: str = ""
    folder: str = ""

    def test_log_groomer_collector_default_enabled(self):
        if self.obj_name == "dag-processor":
            values = {"dagProcessor": {"enabled": True}}
        else:
            values = None

        docs = render_chart(
            values=values, show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"]
        )

        assert len(jmespath.search("spec.template.spec.containers", docs[0])) == 2
        assert f"{self.obj_name}-log-groomer" in [
            c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])
        ]

    def test_log_groomer_collector_can_be_disabled(self):
        if self.obj_name == "dag-processor":
            values = {
                "dagProcessor": {
                    "enabled": True,
                    "logGroomerSidecar": {"enabled": False},
                }
            }
        else:
            values = {f"{self.folder}": {"logGroomerSidecar": {"enabled": False}}}

        docs = render_chart(
            values=values,
            show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"],
        )

        actual = jmespath.search("spec.template.spec.containers", docs[0])

        assert len(actual) == 1

    def test_log_groomer_collector_default_command_and_args(self):
        if self.obj_name == "dag-processor":
            values = {"dagProcessor": {"enabled": True}}
        else:
            values = None

        docs = render_chart(
            values=values, show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"]
        )

        assert jmespath.search("spec.template.spec.containers[1].command", docs[0]) is None
        assert jmespath.search("spec.template.spec.containers[1].args", docs[0]) == ["bash", "/clean-logs"]

    def test_log_groomer_collector_default_retention_days(self):
        if self.obj_name == "dag-processor":
            values = {"dagProcessor": {"enabled": True}}
        else:
            values = None

        docs = render_chart(
            values=values, show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"]
        )

        assert (
            jmespath.search("spec.template.spec.containers[1].env[0].name", docs[0])
            == "AIRFLOW__LOG_RETENTION_DAYS"
        )
        assert jmespath.search("spec.template.spec.containers[1].env[0].value", docs[0]) == "15"

    def test_log_groomer_collector_custom_env(self):
        env = [
            {"name": "APP_RELEASE_NAME", "value": "{{ .Release.Name }}-airflow"},
            {"name": "APP__LOG_RETENTION_DAYS", "value": "5"},
        ]

        if self.obj_name == "dag-processor":
            values = {"dagProcessor": {"enabled": True, "logGroomerSidecar": {"env": env}}}
        else:
            values = {
                "workers": {"logGroomerSidecar": {"env": env}},
                "scheduler": {"logGroomerSidecar": {"env": env}},
                "triggerer": {"logGroomerSidecar": {"env": env}},
            }

        docs = render_chart(
            values=values, show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"]
        )

        assert {"name": "APP_RELEASE_NAME", "value": "release-name-airflow"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {"name": "APP__LOG_RETENTION_DAYS", "value": "5"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_log_groomer_command_and_args_overrides(self, command, args):
        if self.obj_name == "dag-processor":
            values = {
                "dagProcessor": {
                    "enabled": True,
                    "logGroomerSidecar": {"command": command, "args": args},
                }
            }
        else:
            values = {f"{self.folder}": {"logGroomerSidecar": {"command": command, "args": args}}}

        docs = render_chart(
            values=values,
            show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[1].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[1].args", docs[0])

    def test_log_groomer_command_and_args_overrides_are_templated(self):
        if self.obj_name == "dag-processor":
            values = {
                "dagProcessor": {
                    "enabled": True,
                    "logGroomerSidecar": {
                        "command": ["{{ .Release.Name }}"],
                        "args": ["{{ .Release.Service }}"],
                    },
                }
            }
        else:
            values = {
                f"{self.folder}": {
                    "logGroomerSidecar": {
                        "command": ["{{ .Release.Name }}"],
                        "args": ["{{ .Release.Service }}"],
                    }
                }
            }

        docs = render_chart(
            values=values,
            show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[1].command", docs[0]) == ["release-name"]
        assert jmespath.search("spec.template.spec.containers[1].args", docs[0]) == ["Helm"]

    @pytest.mark.parametrize(("retention_days", "retention_result"), [(None, None), (30, "30")])
    def test_log_groomer_retention_days_overrides(self, retention_days, retention_result):
        if self.obj_name == "dag-processor":
            values = {
                "dagProcessor": {"enabled": True, "logGroomerSidecar": {"retentionDays": retention_days}}
            }
        else:
            values = {f"{self.folder}": {"logGroomerSidecar": {"retentionDays": retention_days}}}

        docs = render_chart(
            values=values,
            show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"],
        )

        if retention_result:
            assert (
                jmespath.search(
                    "spec.template.spec.containers[1].env[?name=='AIRFLOW__LOG_RETENTION_DAYS'].value | [0]",
                    docs[0],
                )
                == retention_result
            )
        else:
            assert len(jmespath.search("spec.template.spec.containers[1].env", docs[0])) == 2

    @pytest.mark.parametrize(("frequency_minutes", "frequency_result"), [(None, None), (20, "20")])
    def test_log_groomer_frequency_minutes_overrides(self, frequency_minutes, frequency_result):
        if self.obj_name == "dag-processor":
            values = {
                "dagProcessor": {
                    "enabled": True,
                    "logGroomerSidecar": {"frequencyMinutes": frequency_minutes},
                }
            }
        else:
            values = {f"{self.folder}": {"logGroomerSidecar": {"frequencyMinutes": frequency_minutes}}}

        docs = render_chart(
            values=values,
            show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"],
        )

        if frequency_result:
            assert (
                jmespath.search(
                    "spec.template.spec.containers[1].env[?name=='AIRFLOW__LOG_CLEANUP_FREQUENCY_MINUTES'].value | [0]",
                    docs[0],
                )
                == frequency_result
            )
        else:
            assert len(jmespath.search("spec.template.spec.containers[1].env", docs[0])) == 2

    def test_log_groomer_resources(self):
        if self.obj_name == "dag-processor":
            values = {
                "dagProcessor": {
                    "enabled": True,
                    "logGroomerSidecar": {
                        "resources": {
                            "requests": {"memory": "2Gi", "cpu": "1"},
                            "limits": {"memory": "3Gi", "cpu": "2"},
                        }
                    },
                }
            }
        else:
            values = {
                f"{self.folder}": {
                    "logGroomerSidecar": {
                        "resources": {
                            "requests": {"memory": "2Gi", "cpu": "1"},
                            "limits": {"memory": "3Gi", "cpu": "2"},
                        }
                    }
                }
            }

        docs = render_chart(
            values=values,
            show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[1].resources", docs[0]) == {
            "limits": {
                "cpu": "2",
                "memory": "3Gi",
            },
            "requests": {
                "cpu": "1",
                "memory": "2Gi",
            },
        }

    def test_log_groomer_has_airflow_home(self):
        if self.obj_name == "dag-processor":
            values = {"dagProcessor": {"enabled": True}}
        else:
            values = None

        docs = render_chart(
            values=values, show_only=[f"templates/{self.folder}/{self.obj_name}-deployment.yaml"]
        )

        assert (
            jmespath.search("spec.template.spec.containers[1].env[?name=='AIRFLOW_HOME'].name | [0]", docs[0])
            == "AIRFLOW_HOME"
        )
