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

import unittest

import jmespath
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart


class ConfigmapTest(unittest.TestCase):
    def test_single_annotation(self):
        docs = render_chart(
            values={
                "airflowConfigAnnotations": {"key": "value"},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        annotations = jmespath.search("metadata.annotations", docs[0])
        assert "value" == annotations.get("key")

    def test_multiple_annotations(self):
        docs = render_chart(
            values={
                "airflowConfigAnnotations": {"key": "value", "key-two": "value-two"},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        annotations = jmespath.search("metadata.annotations", docs[0])
        assert "value" == annotations.get("key")
        assert "value-two" == annotations.get("key-two")

    @parameterized.expand(
        [
            ('2.2.0', None, None, True),
            ('2.2.0', "foo", None, False),
            ('2.2.0', None, "foo", False),
            ('2.1.3', None, None, False),
            ('2.1.3', "foo", None, False),
        ]
    )
    def test_default_airflow_local_settings(self, af_version, secret_key, secret_key_name, expected):
        docs = render_chart(
            values={
                "airflowVersion": af_version,
                "webserverSecretKey": secret_key,
                "webserverSecretKeySecretName": secret_key_name,
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )
        if expected:
            assert (
                "Usage of a dynamic webserver secret key detected"
                in jmespath.search('data."airflow_local_settings.py"', docs[0]).strip()
            )
        else:
            assert "" == jmespath.search('data."airflow_local_settings.py"', docs[0]).strip()

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello {{ .Release.Name }}!"},
            show_only=["templates/configmaps/configmap.yaml"],
        )

        assert (
            "# Well hello release-name!"
            == jmespath.search('data."airflow_local_settings.py"', docs[0]).strip()
        )

    def test_kerberos_config_available_with_celery_executor(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "kerberos": {"enabled": True, "config": "krb5content"},
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        assert jmespath.search('data."krb5.conf"', docs[0]) == "\nkrb5content\n"

    def test_pod_template_is_templated(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "podTemplate": """
apiVersion: v1
kind: Pod
metadata:
  name: dummy-name
  labels:
    mylabel: {{ .Release.Name }}
""",
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        pod_template_file = jmespath.search('data."pod_template_file.yaml"', docs[0])
        assert "mylabel: release-name" in pod_template_file
