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

import json
import textwrap
import unittest

import jmespath
import yaml

from tests.helm_template_generator import prepare_k8s_lookup_dict, render_chart

RELEASE_NAME = "TEST-POOLS-CONFIGMAPS"


class DoNotCreatePoolsTest(unittest.TestCase):
    def setUp(self):
        values_str = textwrap.dedent(
            """
            airflowPools: {}
            """
        )
        values = yaml.safe_load(values_str)
        k8s_objects = render_chart(
            RELEASE_NAME,
            values=values,
            show_only=["templates/configmaps/configmap.yaml", "templates/create-pools-job.yaml"],
        )

        self.k8s_objects_by_key = prepare_k8s_lookup_dict(k8s_objects)

        all_expected_keys = [
            ("ConfigMap", f"{RELEASE_NAME}-airflow-config"),
        ]

        assert set(self.k8s_objects_by_key.keys()) == set(all_expected_keys)

    def test_configmap_properly_configured(self):
        configmap_obj = self.k8s_objects_by_key[("ConfigMap", f"{RELEASE_NAME}-airflow-config")]
        assert json.loads(configmap_obj["data"]["pools.json"]) == {}


class CreatePoolsTest(unittest.TestCase):
    def setUp(self):
        values_str = textwrap.dedent(
            """
            airflowPools:
              my-pool-name:
                description: My description
                slots: 1
            """
        )
        values = yaml.safe_load(values_str)
        k8s_objects = render_chart(
            RELEASE_NAME,
            values=values,
            show_only=["templates/configmaps/configmap.yaml", "templates/create-pools-job.yaml"],
        )

        self.k8s_objects_by_key = prepare_k8s_lookup_dict(k8s_objects)

        all_expected_keys = [
            ("ConfigMap", f"{RELEASE_NAME}-airflow-config"),
            ("Job", f"{RELEASE_NAME}-create-pool"),
        ]

        assert set(self.k8s_objects_by_key.keys()) == set(all_expected_keys)

    def test_configmap_properly_configured(self):
        configmap_obj = self.k8s_objects_by_key[("ConfigMap", f"{RELEASE_NAME}-airflow-config")]
        assert json.loads(configmap_obj["data"]["pools.json"]) == {
            "my-pool-name": {"description": "My description", "slots": 1}
        }

    def test_create_pool_job_setup(self):
        job_obj = self.k8s_objects_by_key[("Job", f"{RELEASE_NAME}-create-pool")]
        args = jmespath.search("spec.template.spec.containers[0].args", job_obj)
        assert args == ['bash', '-c', 'airflow pools import "/opt/airflow/pools.json"']
