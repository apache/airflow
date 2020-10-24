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

from tests.helm_template_generator import render_chart

OBJECT_COUNT_IN_BASIC_DEPLOYMENT = 22


class TestBaseChartTest(unittest.TestCase):
    def test_basic_deployments(self):
        k8s_objects = render_chart("TEST-BASIC", {"chart": {'metadata': 'AA'}})
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        self.assertEqual(
            list_of_kind_names_tuples,
            [
                ('ServiceAccount', 'TEST-BASIC-scheduler'),
                ('ServiceAccount', 'TEST-BASIC-webserver'),
                ('ServiceAccount', 'TEST-BASIC-worker'),
                ('Secret', 'TEST-BASIC-postgresql'),
                ('Secret', 'TEST-BASIC-airflow-metadata'),
                ('Secret', 'TEST-BASIC-airflow-result-backend'),
                ('ConfigMap', 'TEST-BASIC-airflow-config'),
                ('ClusterRole', 'TEST-BASIC-pod-launcher-role'),
                ('ClusterRoleBinding', 'TEST-BASIC-pod-launcher-rolebinding'),
                ('Service', 'TEST-BASIC-postgresql-headless'),
                ('Service', 'TEST-BASIC-postgresql'),
                ('Service', 'TEST-BASIC-statsd'),
                ('Service', 'TEST-BASIC-webserver'),
                ('Deployment', 'TEST-BASIC-scheduler'),
                ('Deployment', 'TEST-BASIC-statsd'),
                ('Deployment', 'TEST-BASIC-webserver'),
                ('StatefulSet', 'TEST-BASIC-postgresql'),
                ('Secret', 'TEST-BASIC-fernet-key'),
                ('Secret', 'TEST-BASIC-redis-password'),
                ('Secret', 'TEST-BASIC-broker-url'),
                ('Job', 'TEST-BASIC-create-user'),
                ('Job', 'TEST-BASIC-run-airflow-migrations'),
            ],
        )
        self.assertEqual(OBJECT_COUNT_IN_BASIC_DEPLOYMENT, len(k8s_objects))

    def test_basic_deployment_without_default_users(self):
        k8s_objects = render_chart("TEST-BASIC", {"webserver": {'defaultUser': {'enabled': False}}})
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        self.assertNotIn(('Job', 'TEST-BASIC-create-user'), list_of_kind_names_tuples)
        self.assertEqual(OBJECT_COUNT_IN_BASIC_DEPLOYMENT - 1, len(k8s_objects))
