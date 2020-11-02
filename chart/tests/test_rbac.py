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

from tests.helm_template_generator import render_chart

CLEANUP_DEPLOYMENT_KIND_NAME_TUPLES = [
    ('Secret', 'TEST-RBAC-postgresql'),
    ('Secret', 'TEST-RBAC-airflow-metadata'),
    ('Secret', 'TEST-RBAC-airflow-result-backend'),
    ('ConfigMap', 'TEST-RBAC-airflow-config'),
    ('Service', 'TEST-RBAC-postgresql-headless'),
    ('Service', 'TEST-RBAC-postgresql'),
    ('Service', 'TEST-RBAC-statsd'),
    ('Service', 'TEST-RBAC-webserver'),
    ('Deployment', 'TEST-RBAC-scheduler'),
    ('Deployment', 'TEST-RBAC-statsd'),
    ('Deployment', 'TEST-RBAC-webserver'),
    ('StatefulSet', 'TEST-RBAC-postgresql'),
    ('Secret', 'TEST-RBAC-fernet-key'),
    ('Secret', 'TEST-RBAC-redis-password'),
    ('Secret', 'TEST-RBAC-broker-url'),
    ('Job', 'TEST-RBAC-create-user'),
    ('Job', 'TEST-RBAC-run-airflow-migrations'),
    ('CronJob', 'TEST-RBAC-cleanup'),
]

RBAC_ENABLED_KIND_NAME_TUPLES = [
    ('Role', 'TEST-RBAC-pod-launcher-role'),
    ('Role', 'TEST-RBAC-cleanup-role'),
    ('RoleBinding', 'TEST-RBAC-pod-launcher-rolebinding'),
    ('RoleBinding', 'TEST-RBAC-cleanup-rolebinding'),
]

SERVICE_ACCOUNT_NAME_TUPLES = [
    ('ServiceAccount', 'TEST-RBAC-cleanup'),
    ('ServiceAccount', 'TEST-RBAC-scheduler'),
    ('ServiceAccount', 'TEST-RBAC-webserver'),
    ('ServiceAccount', 'TEST-RBAC-worker'),
]

CUSTOM_SERVICE_ACCOUNT_NAMES = (
    CUSTOM_SCHEDULER_NAME,
    CUSTOM_WEBSERVER_NAME,
    CUSTOM_WORKER_NAME,
    CUSTOM_CLEANUP_NAME,
) = (
    "TestScheduler",
    "TestWebserver",
    "TestWorker",
    "TestCleanup",
)


class RBACTest(unittest.TestCase):
    def test_deployments_no_rbac_no_sa(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "rbac": {"create": False},
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "create": False,
                    },
                },
                "scheduler": {"serviceAccount": {"create": False}},
                "webserver": {"serviceAccount": {"create": False}},
                "workers": {"serviceAccount": {"create": False}},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]

        self.assertCountEqual(
            list_of_kind_names_tuples,
            CLEANUP_DEPLOYMENT_KIND_NAME_TUPLES,
        )

    def test_deployments_no_rbac_with_sa(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "rbac": {"create": False},
                "cleanup": {"enabled": True},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = CLEANUP_DEPLOYMENT_KIND_NAME_TUPLES + SERVICE_ACCOUNT_NAME_TUPLES
        self.assertCountEqual(
            list_of_kind_names_tuples,
            real_list_of_kind_names,
        )

    def test_deployments_with_rbac_no_sa(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "create": False,
                    },
                },
                "scheduler": {"serviceAccount": {"create": False}},
                "webserver": {"serviceAccount": {"create": False}},
                "workers": {"serviceAccount": {"create": False}},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = CLEANUP_DEPLOYMENT_KIND_NAME_TUPLES + RBAC_ENABLED_KIND_NAME_TUPLES
        self.assertCountEqual(
            list_of_kind_names_tuples,
            real_list_of_kind_names,
        )

    def test_deployments_with_rbac_with_sa(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "cleanup": {"enabled": True},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = (
            CLEANUP_DEPLOYMENT_KIND_NAME_TUPLES + SERVICE_ACCOUNT_NAME_TUPLES + RBAC_ENABLED_KIND_NAME_TUPLES
        )
        self.assertCountEqual(
            list_of_kind_names_tuples,
            real_list_of_kind_names,
        )

    def test_service_account_custom_names(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_CLEANUP_NAME,
                    },
                },
                "scheduler": {"serviceAccount": {"name": CUSTOM_SCHEDULER_NAME}},
                "webserver": {"serviceAccount": {"name": CUSTOM_WEBSERVER_NAME}},
                "workers": {"serviceAccount": {"name": CUSTOM_WORKER_NAME}},
            },
        )
        list_of_sa_names = [
            k8s_object['metadata']['name']
            for k8s_object in k8s_objects
            if k8s_object['kind'] == "ServiceAccount"
        ]
        self.assertCountEqual(
            list_of_sa_names,
            CUSTOM_SERVICE_ACCOUNT_NAMES,
        )

    def test_service_account_custom_names_in_objects(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_CLEANUP_NAME,
                    },
                },
                "scheduler": {"serviceAccount": {"name": CUSTOM_SCHEDULER_NAME}},
                "webserver": {"serviceAccount": {"name": CUSTOM_WEBSERVER_NAME}},
                "workers": {"serviceAccount": {"name": CUSTOM_WORKER_NAME}},
                "executor": "CeleryExecutor",  # create worker deployment
            },
        )
        list_of_sa_names_in_objects = []
        for k8s_object in k8s_objects:
            name = (
                jmespath.search("spec.template.spec.serviceAccountName", k8s_object)
                or jmespath.search(
                    "spec.jobTemplate.spec.template.spec.serviceAccountName",
                    k8s_object,
                )
                or None
            )
            if name:
                list_of_sa_names_in_objects.append(name)

        self.assertCountEqual(
            list_of_sa_names_in_objects,
            CUSTOM_SERVICE_ACCOUNT_NAMES,
        )
