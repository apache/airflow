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

import re
from subprocess import CalledProcessError

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart

ES_SECRET_TEMPLATE = "templates/secrets/elasticsearch-secret.yaml"
OS_SECRET_TEMPLATE = "templates/secrets/opensearch-secret.yaml"
SCHEDULER_DEPLOYMENT_TEMPLATE = "templates/scheduler/scheduler-deployment.yaml"
CONFIGMAP_TEMPLATE = "templates/configmaps/configmap.yaml"

CORE_CFG_REGEX = re.compile(r"\[core]\n.*?\n\n", flags=re.RegexFlag.DOTALL)
LOGGING_CFG_REGEX = re.compile(r"\[logging]\n.*?\n\n", flags=re.RegexFlag.DOTALL)


class TestElasticsearchConfig:
    """Tests elasticsearch configuration behaviors."""

    def test_should_not_generate_secret_document_if_elasticsearch_disabled(self):
        docs = render_chart(
            values={"elasticsearch": {"enabled": False}},
            show_only=[ES_SECRET_TEMPLATE],
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
                show_only=[ES_SECRET_TEMPLATE],
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
                show_only=[ES_SECRET_TEMPLATE],
            )
        assert (
            "You must not set both values elasticsearch.secretName and elasticsearch.connection"
            in ex_ctx.value.stderr.decode()
        )

    def test_scheduler_should_add_log_port_when_local_executor_and_elasticsearch_disabled(self):
        docs = render_chart(
            values={"executor": "LocalExecutor"},
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.template.spec.containers[0].ports", docs[0]) == [
            {"name": "worker-logs", "containerPort": 8793}
        ]

    def test_scheduler_should_omit_log_port_when_elasticsearch_enabled(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        assert "ports" not in jmespath.search("spec.template.spec.containers[0]", docs[0])

    def test_env_should_omit_elasticsearch_host_var_if_es_disabled(self):
        docs = render_chart(
            values={},
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        scheduler_env_keys = jmespath.search("spec.template.spec.containers[0].env[*].name", docs[0])
        assert "AIRFLOW__ELASTICSEARCH__HOST" not in scheduler_env_keys

    def test_env_should_add_elasticsearch_host_var_if_es_enabled(self):
        docs = render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        scheduler_env = jmespath.search("spec.template.spec.containers[0].env", docs[0])

        assert {
            "name": "AIRFLOW__ELASTICSEARCH__HOST",
            "valueFrom": {"secretKeyRef": {"name": "test-elastic-secret", "key": "connection"}},
        } in scheduler_env

    def test_env_should_omit_elasticsearch_host_var_if_es_disabled_legacy(self):
        """AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST was the environment key prior to Airflow 1.10.4
        (see https://github.com/apache/airflow/pull/5048), this test can be removed when the Helm chart
        no longer supports Airflow 1.10.3"""
        docs = render_chart(
            values={},
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        scheduler_env_keys = jmespath.search("spec.template.spec.containers[0].env[*].name", docs[0])
        assert "AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST" not in scheduler_env_keys

    def test_env_should_add_elasticsearch_host_var_if_es_enabled_legacy(self):
        """AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST was the environment key prior to Airflow 1.10.4
        (see https://github.com/apache/airflow/pull/5048), this test can be removed when the Helm chart
        no longer supports Airflow 1.10.3"""
        docs = render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        scheduler_env = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        assert {
            "name": "AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST",
            "valueFrom": {"secretKeyRef": {"name": "test-elastic-secret", "key": "connection"}},
        } in scheduler_env

    def test_airflow_cfg_should_set_remote_logging_false_if_es_disabled(self):
        docs = render_chart(
            values={},
            show_only=[CONFIGMAP_TEMPLATE],
        )

        airflow_cfg_text = jmespath.search('data."airflow.cfg"', docs[0])

        logging_lines = LOGGING_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = False" in logging_lines

    def test_airflow_cfg_should_set_remote_logging_true_if_es_enabled(self):
        docs = render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[CONFIGMAP_TEMPLATE],
        )

        airflow_cfg_text = jmespath.search('data."airflow.cfg"', docs[0])

        logging_lines = LOGGING_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = True" in logging_lines

    def test_airflow_cfg_should_set_remote_logging_false_if_es_disabled_legacy(self):
        """core.remote_logging was the config location prior to Airflow 2.0.0, this test can be removed
        when the Helm chart no longer supports Airflow 1.x"""
        docs = render_chart(
            values={},
            show_only=[CONFIGMAP_TEMPLATE],
        )

        airflow_cfg_text = jmespath.search('data."airflow.cfg"', docs[0])

        core_lines = CORE_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = False" in core_lines

    def test_airflow_cfg_should_set_remote_logging_true_if_es_enabled_legacy(self):
        """core.remote_logging was the config location prior to Airflow 2.0.0, this test can be removed
        when the Helm chart no longer supports Airflow 1.x"""
        docs = render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[CONFIGMAP_TEMPLATE],
        )

        airflow_cfg_text = jmespath.search('data."airflow.cfg"', docs[0])

        core_lines = CORE_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = True" in core_lines


class TestOpenSearchConfig:
    """Tests opensearch configuration behaviors."""

    def test_should_not_generate_secret_document_if_opensearch_disabled(self):
        docs = render_chart(
            values={"opensearch": {"enabled": False}},
            show_only=[OS_SECRET_TEMPLATE],
        )

        assert len(docs) == 0

    def test_should_raise_error_when_connection_not_provided(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                values={
                    "opensearch": {
                        "enabled": True,
                    }
                },
                show_only=[OS_SECRET_TEMPLATE],
            )
        assert (
            "You must set one of the values opensearch.secretName or opensearch.connection "
            "when using OpenSearch" in ex_ctx.value.stderr.decode()
        )

    def test_should_raise_error_when_conflicting_options(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                values={
                    "opensearch": {
                        "enabled": True,
                        "secretName": "my-test",
                        "connection": {
                            "user": "username!@#$%%^&*()",
                            "pass": "password!@#$%%^&*()",
                            "host": "opensearchhostname",
                        },
                    },
                },
                show_only=[OS_SECRET_TEMPLATE],
            )
        assert (
            "You must not set both values opensearch.secretName and opensearch.connection"
            in ex_ctx.value.stderr.decode()
        )

    def test_scheduler_should_add_log_port_when_local_executor_and_opensearch_disabled(self):
        docs = render_chart(
            values={"executor": "LocalExecutor"},
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.template.spec.containers[0].ports", docs[0]) == [
            {"name": "worker-logs", "containerPort": 8793}
        ]

    def test_scheduler_should_omit_log_port_when_opensearch_enabled(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",
                "opensearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        assert "ports" not in jmespath.search("spec.template.spec.containers[0]", docs[0])

    def test_env_should_omit_opensearch_host_var_if_os_disabled(self):
        docs = render_chart(
            values={},
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        scheduler_env_keys = jmespath.search("spec.template.spec.containers[0].env[*].name", docs[0])
        assert "AIRFLOW__OPENSEARCH__HOST" not in scheduler_env_keys

    def test_env_should_add_opensearch_host_var_if_os_enabled(self):
        docs = render_chart(
            values={
                "opensearch": {
                    "enabled": True,
                    "secretName": "test-opensearch-secret",
                },
            },
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )

        scheduler_env = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        assert {
            "name": "AIRFLOW__OPENSEARCH__HOST",
            "valueFrom": {"secretKeyRef": {"name": "test-opensearch-secret", "key": "connection"}},
        } in scheduler_env

    def test_airflow_cfg_should_set_remote_logging_false_if_os_disabled(self):
        docs = render_chart(
            values={},
            show_only=[CONFIGMAP_TEMPLATE],
        )

        airflow_cfg_text = jmespath.search('data."airflow.cfg"', docs[0])

        core_lines = CORE_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = False" in core_lines

        logging_lines = LOGGING_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = False" in logging_lines

    def test_airflow_cfg_should_set_remote_logging_true_if_os_enabled(self):
        docs = render_chart(
            values={
                "opensearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[CONFIGMAP_TEMPLATE],
        )

        airflow_cfg_text = jmespath.search('data."airflow.cfg"', docs[0])

        core_lines = CORE_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = True" in core_lines

        logging_lines = LOGGING_CFG_REGEX.findall(airflow_cfg_text)[0].strip().splitlines()
        assert "remote_logging = True" in logging_lines


def test_should_raise_error_when_both_elasticsearch_and_opensearch_enabled():
    with pytest.raises(CalledProcessError) as ex_ctx:
        render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
                "opensearch": {
                    "enabled": True,
                    "secretName": "test-elastic-secret",
                },
            },
            show_only=[SCHEDULER_DEPLOYMENT_TEMPLATE],
        )
    assert (
        "You must not set both values elasticsearch.enabled and opensearch.enabled"
        in ex_ctx.value.stderr.decode()
    )
