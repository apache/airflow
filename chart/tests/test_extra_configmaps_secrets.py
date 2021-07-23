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

import textwrap
import unittest
from base64 import b64encode

import yaml

from tests.helm_template_generator import prepare_k8s_lookup_dict, render_chart

RELEASE_NAME = "TEST-EXTRA-CONFIGMAPS-SECRETS"


class ExtraConfigMapsSecretsTest(unittest.TestCase):
    def test_extra_configmaps(self):
        values_str = textwrap.dedent(
            """
            extraConfigMaps:
              "{{ .Release.Name }}-airflow-variables":
                data: |
                  AIRFLOW_VAR_HELLO_MESSAGE: "Hi!"
                  AIRFLOW_VAR_KUBERNETES_NAMESPACE: "{{ .Release.Namespace }}"
              "{{ .Release.Name }}-other-variables":
                data: |
                  HELLO_WORLD: "Hi again!"
            """
        )
        values = yaml.safe_load(values_str)
        k8s_objects = render_chart(
            RELEASE_NAME, values=values, show_only=["templates/configmaps/extra-configmaps.yaml"]
        )
        k8s_objects_by_key = prepare_k8s_lookup_dict(k8s_objects)

        all_expected_keys = [
            ("ConfigMap", f"{RELEASE_NAME}-airflow-variables"),
            ("ConfigMap", f"{RELEASE_NAME}-other-variables"),
        ]
        assert set(k8s_objects_by_key.keys()) == set(all_expected_keys)

        all_expected_data = [
            {"AIRFLOW_VAR_HELLO_MESSAGE": "Hi!", "AIRFLOW_VAR_KUBERNETES_NAMESPACE": "default"},
            {"HELLO_WORLD": "Hi again!"},
        ]
        for expected_key, expected_data in zip(all_expected_keys, all_expected_data):
            configmap_obj = k8s_objects_by_key[expected_key]
            assert configmap_obj["data"] == expected_data

    def test_extra_secrets(self):
        values_str = textwrap.dedent(
            """
            extraSecrets:
              "{{ .Release.Name }}-airflow-connections":
                data: |
                  AIRFLOW_CON_AWS: {{ printf "aws_connection_string" | b64enc }}
                stringData: |
                  AIRFLOW_CON_GCP: "gcp_connection_string"
              "{{ .Release.Name }}-other-secrets":
                data: |
                  MY_SECRET_1: {{ printf "MY_SECRET_1" | b64enc }}
                  MY_SECRET_2: {{ printf "MY_SECRET_2" | b64enc }}
                stringData: |
                  MY_SECRET_3: "MY_SECRET_3"
                  MY_SECRET_4: "MY_SECRET_4"
            """
        )
        values = yaml.safe_load(values_str)
        k8s_objects = render_chart(
            RELEASE_NAME, values=values, show_only=["templates/secrets/extra-secrets.yaml"]
        )
        k8s_objects_by_key = prepare_k8s_lookup_dict(k8s_objects)

        all_expected_keys = [
            ("Secret", f"{RELEASE_NAME}-airflow-connections"),
            ("Secret", f"{RELEASE_NAME}-other-secrets"),
        ]
        assert set(k8s_objects_by_key.keys()) == set(all_expected_keys)

        all_expected_data = [
            {"AIRFLOW_CON_AWS": b64encode(b"aws_connection_string").decode("utf-8")},
            {
                "MY_SECRET_1": b64encode(b"MY_SECRET_1").decode("utf-8"),
                "MY_SECRET_2": b64encode(b"MY_SECRET_2").decode("utf-8"),
            },
        ]

        all_expected_string_data = [
            {"AIRFLOW_CON_GCP": "gcp_connection_string"},
            {"MY_SECRET_3": "MY_SECRET_3", "MY_SECRET_4": "MY_SECRET_4"},
        ]
        for expected_key, expected_data, expected_string_data in zip(
            all_expected_keys, all_expected_data, all_expected_string_data
        ):
            configmap_obj = k8s_objects_by_key[expected_key]
            assert configmap_obj["data"] == expected_data
            assert configmap_obj["stringData"] == expected_string_data
