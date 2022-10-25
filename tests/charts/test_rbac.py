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

from tests.charts.helm_template_generator import render_chart

DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES = [
    ("Secret", "test-rbac-postgresql"),
    ("Secret", "test-rbac-airflow-metadata"),
    ("Secret", "test-rbac-pgbouncer-config"),
    ("Secret", "test-rbac-pgbouncer-stats"),
    ("ConfigMap", "test-rbac-airflow-config"),
    ("ConfigMap", "test-rbac-statsd"),
    ("Service", "test-rbac-postgresql-headless"),
    ("Service", "test-rbac-postgresql"),
    ("Service", "test-rbac-statsd"),
    ("Service", "test-rbac-webserver"),
    ("Service", "test-rbac-flower"),
    ("Service", "test-rbac-pgbouncer"),
    ("Service", "test-rbac-redis"),
    ("Service", "test-rbac-worker"),
    ("Deployment", "test-rbac-scheduler"),
    ("Deployment", "test-rbac-statsd"),
    ("Deployment", "test-rbac-webserver"),
    ("Deployment", "test-rbac-flower"),
    ("Deployment", "test-rbac-pgbouncer"),
    ("Deployment", "test-rbac-triggerer"),
    ("StatefulSet", "test-rbac-postgresql"),
    ("StatefulSet", "test-rbac-redis"),
    ("StatefulSet", "test-rbac-worker"),
    ("Secret", "test-rbac-broker-url"),
    ("Secret", "test-rbac-fernet-key"),
    ("Secret", "test-rbac-redis-password"),
    ("Secret", "test-rbac-webserver-secret-key"),
    ("Job", "test-rbac-create-user"),
    ("Job", "test-rbac-run-airflow-migrations"),
    ("CronJob", "test-rbac-cleanup"),
]

RBAC_ENABLED_KIND_NAME_TUPLES = [
    ("Role", "test-rbac-pod-launcher-role"),
    ("Role", "test-rbac-cleanup-role"),
    ("Role", "test-rbac-pod-log-reader-role"),
    ("RoleBinding", "test-rbac-pod-launcher-rolebinding"),
    ("RoleBinding", "test-rbac-pod-log-reader-rolebinding"),
    ("RoleBinding", "test-rbac-cleanup-rolebinding"),
]

SERVICE_ACCOUNT_NAME_TUPLES = [
    ("ServiceAccount", "test-rbac-cleanup"),
    ("ServiceAccount", "test-rbac-scheduler"),
    ("ServiceAccount", "test-rbac-webserver"),
    ("ServiceAccount", "test-rbac-worker"),
    ("ServiceAccount", "test-rbac-triggerer"),
    ("ServiceAccount", "test-rbac-pgbouncer"),
    ("ServiceAccount", "test-rbac-flower"),
    ("ServiceAccount", "test-rbac-statsd"),
    ("ServiceAccount", "test-rbac-create-user-job"),
    ("ServiceAccount", "test-rbac-migrate-database-job"),
    ("ServiceAccount", "test-rbac-redis"),
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


class TestRBAC:
    def _get_values_with_version(self, values, version):
        if version != "default":
            values["airflowVersion"] = version
        return values

    def _get_object_count(self, version):
        if version == "2.3.2":
            return [
                ("Secret", "test-rbac-airflow-result-backend")
            ] + DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES
        return DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_deployments_no_rbac_no_sa(self, version):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "test-rbac",
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
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        assert sorted(list_of_kind_names_tuples) == sorted(self._get_object_count(version))

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_deployments_no_rbac_with_sa(self, version):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "test-rbac",
                    "rbac": {"create": False},
                    "cleanup": {"enabled": True},
                    "flower": {"enabled": True},
                    "pgbouncer": {"enabled": True},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = self._get_object_count(version) + SERVICE_ACCOUNT_NAME_TUPLES
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_deployments_with_rbac_no_sa(self, version):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "test-rbac",
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
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = self._get_object_count(version) + RBAC_ENABLED_KIND_NAME_TUPLES
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_deployments_with_rbac_with_sa(self, version):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "test-rbac",
                    "cleanup": {"enabled": True},
                    "flower": {"enabled": True},
                    "pgbouncer": {"enabled": True},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = (
            self._get_object_count(version) + SERVICE_ACCOUNT_NAME_TUPLES + RBAC_ENABLED_KIND_NAME_TUPLES
        )
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    def test_service_account_custom_names(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
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
            k8s_object["metadata"]["name"]
            for k8s_object in k8s_objects
            if k8s_object["kind"] == "ServiceAccount"
        ]
        assert sorted(list_of_sa_names) == sorted(CUSTOM_SERVICE_ACCOUNT_NAMES)

    def test_service_account_custom_names_in_objects(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
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

        assert sorted(list_of_sa_names_in_objects) == sorted(CUSTOM_SERVICE_ACCOUNT_NAMES)

    def test_service_account_without_resource(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
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
            k8s_object["metadata"]["name"]
            for k8s_object in k8s_objects
            if k8s_object["kind"] == "ServiceAccount"
        ]
        service_account_names = [
            "test-rbac-scheduler",
            "test-rbac-webserver",
            "test-rbac-triggerer",
            "test-rbac-migrate-database-job",
        ]
        assert sorted(list_of_sa_names) == sorted(service_account_names)
