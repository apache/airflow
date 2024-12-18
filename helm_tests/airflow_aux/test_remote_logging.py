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

import base64
from subprocess import CalledProcessError

import jmespath
import pytest

from tests.charts.helm_template_generator import render_chart


class TestElasticsearchConfig:
    """Tests elasticsearch configuration behaviors."""

    def test_should_not_generate_a_secret_document_if_elasticsearch_disabled(self):
        docs = render_chart(
            values={"elasticsearch": {"enabled": False}},
            show_only=["templates/secrets/elasticsearch-secret.yaml"],
        )

        assert len(docs) == 0

    def test_should_raise_error_when_connection_not_provided(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                values={
                    "elasticsearch": {
                        "enabled": True,
                    }
                },
                show_only=["templates/secrets/elasticsearch-secret.yaml"],
            )
        assert (
            "You must set one of the values elasticsearch.secretName or elasticsearch.connection "
            "when using a Elasticsearch" in ex_ctx.value.stderr.decode()
        )

    def test_should_raise_error_when_conflicting_options(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                values={
                    "elasticsearch": {
                        "enabled": True,
                        "secretName": "my-test",
                        "connection": {
                            "user": "username!@#$%%^&*()",
                            "pass": "password!@#$%%^&*()",
                            "host": "elastichostname",
                        },
                    },
                },
                show_only=["templates/secrets/elasticsearch-secret.yaml"],
            )
        assert (
            "You must not set both values elasticsearch.secretName and elasticsearch.connection"
            in ex_ctx.value.stderr.decode()
        )

    def test_scheduler_should_add_log_port_when_local_executor_and_elasticsearch_disabled(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",
                "elasticsearch": {"enabled": False}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].ports", docs[0]) == [{
            "name": "worker-logs",
            "containerPort": 8793
        }]

    def test_scheduler_should_omit_log_port_when_elasticsearch_enabled(self):
        docs = render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "ports" not in jmespath.search("spec.template.spec.containers[0]", docs[0])

    def test_env_should_omit_elasticsearch_host_vars_if_es_disabled(self):
        docs = render_chart(
            values={},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        container_env = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        container_env_keys = [entry["name"] for entry in container_env]

        assert "AIRFLOW__ELASTICSEARCH__HOST" not in container_env_keys
        assert "AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST" not in container_env_keys

    def test_env_should_add_elasticsearch_host_vars_if_es_enabled(self):
        docs = render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        expected_value_from = {
            "secretKeyRef": {
                "name": "test-elastic-secret",
                "key": "connection"
            }
        }

        container_env = jmespath.search("spec.template.spec.containers[0].env", docs[0])

        assert {"name": "AIRFLOW__ELASTICSEARCH__HOST", "valueFrom": expected_value_from} in container_env
        assert {"name": "AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST",
                "valueFrom": expected_value_from} in container_env
