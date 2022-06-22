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

DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES = [
    ('Secret', 'TEST-RBAC-postgresql'),
    ('Secret', 'TEST-RBAC-airflow-metadata'),
    ('Secret', 'TEST-RBAC-pgbouncer-config'),
    ('Secret', 'TEST-RBAC-pgbouncer-stats'),
    ('ConfigMap', 'TEST-RBAC-airflow-config'),
    ('Service', 'TEST-RBAC-postgresql-headless'),
    ('Service', 'TEST-RBAC-postgresql'),
    ('Service', 'TEST-RBAC-statsd'),
    ('Service', 'TEST-RBAC-webserver'),
    ('Service', 'TEST-RBAC-flower'),
    ('Service', 'TEST-RBAC-pgbouncer'),
    ('Service', 'TEST-RBAC-redis'),
    ('Service', 'TEST-RBAC-worker'),
    ('Deployment', 'TEST-RBAC-scheduler'),
    ('Deployment', 'TEST-RBAC-statsd'),
    ('Deployment', 'TEST-RBAC-webserver'),
    ('Deployment', 'TEST-RBAC-flower'),
    ('Deployment', 'TEST-RBAC-pgbouncer'),
    ('Deployment', 'TEST-RBAC-triggerer'),
    ('StatefulSet', 'TEST-RBAC-postgresql'),
    ('StatefulSet', 'TEST-RBAC-redis'),
    ('StatefulSet', 'TEST-RBAC-worker'),
    ('Secret', 'TEST-RBAC-broker-url'),
    ('Secret', 'TEST-RBAC-fernet-key'),
    ('Secret', 'TEST-RBAC-redis-password'),
    ('Secret', 'TEST-RBAC-webserver-secret-key'),
    ('Job', 'TEST-RBAC-create-user'),
    ('Job', 'TEST-RBAC-run-airflow-migrations'),
    ('CronJob', 'TEST-RBAC-cleanup'),
]

RBAC_ENABLED_KIND_NAME_TUPLES = [
    ('Role', 'TEST-RBAC-pod-launcher-role'),
    ('Role', 'TEST-RBAC-cleanup-role'),
    ('Role', 'TEST-RBAC-pod-log-reader-role'),
    ('RoleBinding', 'TEST-RBAC-pod-launcher-rolebinding'),
    ('RoleBinding', 'TEST-RBAC-pod-log-reader-rolebinding'),
    ('RoleBinding', 'TEST-RBAC-cleanup-rolebinding'),
]

SERVICE_ACCOUNT_NAME_TUPLES = [
    ('ServiceAccount', 'TEST-RBAC-cleanup'),
    ('ServiceAccount', 'TEST-RBAC-scheduler'),
    ('ServiceAccount', 'TEST-RBAC-webserver'),
    ('ServiceAccount', 'TEST-RBAC-worker'),
    ('ServiceAccount', 'TEST-RBAC-triggerer'),
    ('ServiceAccount', 'TEST-RBAC-pgbouncer'),
    ('ServiceAccount', 'TEST-RBAC-flower'),
    ('ServiceAccount', 'TEST-RBAC-statsd'),
    ('ServiceAccount', 'TEST-RBAC-create-user-job'),
    ('ServiceAccount', 'TEST-RBAC-migrate-database-job'),
    ('ServiceAccount', 'TEST-RBAC-redis'),
]

CUSTOM_SERVICE_ACCOUNT_NAMES = (
    CUSTOM_SCHEDULER_NAME,
    CUSTOM_WEBSERVER_NAME,
    CUSTOM_WORKER_NAME,
    CUSTOM_TRIGGERER_NAME,
    CUSTOM_CLEANUP_NAME,
    CUSTOM_FLOWER_NAME,
    CUSTOM_PGBOUNCER_NAME,
    CUSTOM_STATSD_NAME,
    CUSTOM_CREATE_USER_JOBS_NAME,
    CUSTOM_MIGRATE_DATABASE_JOBS_NAME,
    CUSTOM_REDIS_NAME,
) = (
    "TestScheduler",
    "TestWebserver",
    "TestWorker",
    "TestTriggerer",
    "TestCleanup",
    "TestFlower",
    "TestPGBouncer",
    "TestStatsd",
    "TestCreateUserJob",
    "TestMigrateDatabaseJob",
    "TestRedis",
)


class RBACTest(unittest.TestCase):
    def _get_values_with_version(self, values, version):
        if version != "default":
            values["airflowVersion"] = version
        return values

    def _get_object_count(self, version):
        # TODO remove default from condition after airflow update
        if version == "2.3.2" or version == "default":
            return [
                ('Secret', 'TEST-RBAC-airflow-result-backend')
            ] + DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES
        return DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_deployments_no_rbac_no_sa(self, version):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "TEST-RBAC",
                    "rbac": {"create": False},
                    "cleanup": {
                        "enabled": True,
                        "serviceAccount": {
                            "create": False,
                        },
                    },
                    "pgbouncer": {
                        "enabled": True,
                        "serviceAccount": {
                            "create": False,
                        },
                    },
                    "redis": {"serviceAccount": {"create": False}},
                    "scheduler": {"serviceAccount": {"create": False}},
                    "webserver": {"serviceAccount": {"create": False}},
                    "workers": {"serviceAccount": {"create": False}},
                    "triggerer": {"serviceAccount": {"create": False}},
                    "statsd": {"serviceAccount": {"create": False}},
                    "createUserJob": {"serviceAccount": {"create": False}},
                    "migrateDatabaseJob": {"serviceAccount": {"create": False}},
                    "flower": {"enabled": True, "serviceAccount": {"create": False}},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]

        self.assertCountEqual(
            list_of_kind_names_tuples,
            self._get_object_count(version),
        )

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_deployments_no_rbac_with_sa(self, version):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "TEST-RBAC",
                    "rbac": {"create": False},
                    "cleanup": {"enabled": True},
                    "flower": {"enabled": True},
                    "pgbouncer": {"enabled": True},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = self._get_object_count(version) + SERVICE_ACCOUNT_NAME_TUPLES
        self.assertCountEqual(
            list_of_kind_names_tuples,
            real_list_of_kind_names,
        )

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_deployments_with_rbac_no_sa(self, version):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "TEST-RBAC",
                    "cleanup": {
                        "enabled": True,
                        "serviceAccount": {
                            "create": False,
                        },
                    },
                    "scheduler": {"serviceAccount": {"create": False}},
                    "webserver": {"serviceAccount": {"create": False}},
                    "workers": {"serviceAccount": {"create": False}},
                    "triggerer": {"serviceAccount": {"create": False}},
                    "flower": {"enabled": True, "serviceAccount": {"create": False}},
                    "statsd": {"serviceAccount": {"create": False}},
                    "redis": {"serviceAccount": {"create": False}},
                    "pgbouncer": {
                        "enabled": True,
                        "serviceAccount": {
                            "create": False,
                        },
                    },
                    "createUserJob": {"serviceAccount": {"create": False}},
                    "migrateDatabaseJob": {"serviceAccount": {"create": False}},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = self._get_object_count(version) + RBAC_ENABLED_KIND_NAME_TUPLES
        self.assertCountEqual(
            list_of_kind_names_tuples,
            real_list_of_kind_names,
        )

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_deployments_with_rbac_with_sa(self, version):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "TEST-RBAC",
                    "cleanup": {"enabled": True},
                    "flower": {"enabled": True},
                    "pgbouncer": {"enabled": True},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = (
            self._get_object_count(version) + SERVICE_ACCOUNT_NAME_TUPLES + RBAC_ENABLED_KIND_NAME_TUPLES
        )
        self.assertCountEqual(
            list_of_kind_names_tuples,
            real_list_of_kind_names,
        )

    def test_service_account_custom_names(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "fullnameOverride": "TEST-RBAC",
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_CLEANUP_NAME,
                    },
                },
                "scheduler": {"serviceAccount": {"name": CUSTOM_SCHEDULER_NAME}},
                "webserver": {"serviceAccount": {"name": CUSTOM_WEBSERVER_NAME}},
                "workers": {"serviceAccount": {"name": CUSTOM_WORKER_NAME}},
                "triggerer": {"serviceAccount": {"name": CUSTOM_TRIGGERER_NAME}},
                "flower": {"enabled": True, "serviceAccount": {"name": CUSTOM_FLOWER_NAME}},
                "statsd": {"serviceAccount": {"name": CUSTOM_STATSD_NAME}},
                "redis": {"serviceAccount": {"name": CUSTOM_REDIS_NAME}},
                "pgbouncer": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_PGBOUNCER_NAME,
                    },
                },
                "createUserJob": {"serviceAccount": {"name": CUSTOM_CREATE_USER_JOBS_NAME}},
                "migrateDatabaseJob": {"serviceAccount": {"name": CUSTOM_MIGRATE_DATABASE_JOBS_NAME}},
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
                "fullnameOverride": "TEST-RBAC",
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_CLEANUP_NAME,
                    },
                },
                "scheduler": {"serviceAccount": {"name": CUSTOM_SCHEDULER_NAME}},
                "webserver": {"serviceAccount": {"name": CUSTOM_WEBSERVER_NAME}},
                "workers": {"serviceAccount": {"name": CUSTOM_WORKER_NAME}},
                "triggerer": {"serviceAccount": {"name": CUSTOM_TRIGGERER_NAME}},
                "flower": {"enabled": True, "serviceAccount": {"name": CUSTOM_FLOWER_NAME}},
                "statsd": {"serviceAccount": {"name": CUSTOM_STATSD_NAME}},
                "redis": {"serviceAccount": {"name": CUSTOM_REDIS_NAME}},
                "pgbouncer": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_PGBOUNCER_NAME,
                    },
                },
                "createUserJob": {"serviceAccount": {"name": CUSTOM_CREATE_USER_JOBS_NAME}},
                "migrateDatabaseJob": {"serviceAccount": {"name": CUSTOM_MIGRATE_DATABASE_JOBS_NAME}},
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
            if name and name not in list_of_sa_names_in_objects:
                list_of_sa_names_in_objects.append(name)

        self.assertCountEqual(
            list_of_sa_names_in_objects,
            CUSTOM_SERVICE_ACCOUNT_NAMES,
        )

    def test_service_account_without_resource(self):
        k8s_objects = render_chart(
            "TEST-RBAC",
            values={
                "fullnameOverride": "TEST-RBAC",
                "executor": "LocalExecutor",
                "cleanup": {"enabled": False},
                "pgbouncer": {"enabled": False},
                "redis": {"enabled": False},
                "flower": {"enabled": False},
                "statsd": {"enabled": False},
                "webserver": {"defaultUser": {"enabled": False}},
            },
        )
        list_of_sa_names = [
            k8s_object['metadata']['name']
            for k8s_object in k8s_objects
            if k8s_object['kind'] == "ServiceAccount"
        ]
        service_account_names = [
            'TEST-RBAC-scheduler',
            'TEST-RBAC-webserver',
            'TEST-RBAC-triggerer',
            'TEST-RBAC-migrate-database-job',
        ]
        self.assertCountEqual(list_of_sa_names, service_account_names)
