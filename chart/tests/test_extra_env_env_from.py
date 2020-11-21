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

import jmespath
import yaml
from parameterized import parameterized

from tests.helm_template_generator import prepare_k8s_lookup_dict, render_chart

RELEASE_NAME = "TEST-EXTRA-ENV-ENV-FROM"

# Test Params: k8s object key and paths with expected env / envFrom
PARAMS = [
    (
        ("Job", f"{RELEASE_NAME}-create-user"),
        ("spec.template.spec.containers[0]",),
    ),
    (
        ("Job", f"{RELEASE_NAME}-run-airflow-migrations"),
        ("spec.template.spec.containers[0]",),
    ),
    (
        ("Deployment", f"{RELEASE_NAME}-scheduler"),
        (
            "spec.template.spec.initContainers[0]",
            "spec.template.spec.containers[0]",
        ),
    ),
    (
        ("StatefulSet", f"{RELEASE_NAME}-worker"),
        (
            "spec.template.spec.initContainers[0]",
            "spec.template.spec.containers[0]",
        ),
    ),
    (
        ("Deployment", f"{RELEASE_NAME}-webserver"),
        ("spec.template.spec.initContainers[0]", "spec.template.spec.containers[0]"),
    ),
]


class ExtraEnvEnvFromTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        values_str = textwrap.dedent(
            """
            executor: "CeleryExecutor"
            extraEnvFrom: |
              - secretRef:
                  name: '{{ .Release.Name }}-airflow-connections'
              - configMapRef:
                  name: '{{ .Release.Name }}-airflow-variables'
            extraEnv: |
              - name: PLATFORM
                value: FR
              - name: TEST
                valueFrom:
                  secretKeyRef:
                    name: '{{ .Release.Name }}-some-secret'
                    key: connection
            """
        )
        values = yaml.safe_load(values_str)
        cls.k8s_objects = render_chart(RELEASE_NAME, values=values)
        cls.k8s_objects_by_key = prepare_k8s_lookup_dict(cls.k8s_objects)

    @parameterized.expand(PARAMS)
    def test_extra_env(self, k8s_obj_key, env_paths):
        expected_env_as_str = textwrap.dedent(
            f"""
            - name: PLATFORM
              value: FR
            - name: TEST
              valueFrom:
                secretKeyRef:
                  key: connection
                  name: {RELEASE_NAME}-some-secret
            """
        ).lstrip()
        k8s_object = self.k8s_objects_by_key[k8s_obj_key]
        for path in env_paths:
            env = jmespath.search(f"{path}.env", k8s_object)
            self.assertIn(expected_env_as_str, yaml.dump(env))

    @parameterized.expand(PARAMS)
    def test_extra_env_from(self, k8s_obj_key, env_from_paths):
        expected_env_from_as_str = textwrap.dedent(
            f"""
            - secretRef:
                name: {RELEASE_NAME}-airflow-connections
            - configMapRef:
                name: {RELEASE_NAME}-airflow-variables
            """
        ).lstrip()

        k8s_object = self.k8s_objects_by_key[k8s_obj_key]
        for path in env_from_paths:
            env_from = jmespath.search(f"{path}.envFrom", k8s_object)
            self.assertIn(expected_env_from_as_str, yaml.dump(env_from))
