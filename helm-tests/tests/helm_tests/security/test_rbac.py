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

from copy import copy

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart

DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES = [
    ("Secret", "test-rbac-postgresql"),
    ("Secret", "test-rbac-metadata"),
    ("Secret", "test-rbac-pgbouncer-config"),
    ("Secret", "test-rbac-pgbouncer-stats"),
    ("ConfigMap", "test-rbac-config"),
    ("ConfigMap", "test-rbac-statsd"),
    ("Service", "test-rbac-api-server"),
    ("Service", "test-rbac-postgresql-hl"),
    ("Service", "test-rbac-postgresql"),
    ("Service", "test-rbac-statsd"),
    ("Service", "test-rbac-flower"),
    ("Service", "test-rbac-pgbouncer"),
    ("Service", "test-rbac-redis"),
    ("Service", "test-rbac-triggerer"),
    ("Service", "test-rbac-worker"),
    ("Deployment", "test-rbac-api-server"),
    ("Deployment", "test-rbac-dag-processor"),
    ("Deployment", "test-rbac-scheduler"),
    ("Deployment", "test-rbac-statsd"),
    ("Deployment", "test-rbac-flower"),
    ("Deployment", "test-rbac-pgbouncer"),
    ("StatefulSet", "test-rbac-postgresql"),
    ("StatefulSet", "test-rbac-redis"),
    ("StatefulSet", "test-rbac-triggerer"),
    ("StatefulSet", "test-rbac-worker"),
    ("Secret", "test-rbac-api-secret-key"),
    ("Secret", "test-rbac-broker-url"),
    ("Secret", "test-rbac-fernet-key"),
    ("Secret", "test-rbac-jwt-secret"),
    ("Secret", "test-rbac-redis-password"),
    ("Job", "test-rbac-create-user"),
    ("Job", "test-rbac-run-airflow-migrations"),
    ("CronJob", "test-rbac-cleanup"),
    ("CronJob", "test-rbac-database-cleanup"),
]

RBAC_ENABLED_KIND_NAME_TUPLES = [
    ("Role", "test-rbac-pod-launcher-role"),
    ("Role", "test-rbac-cleanup-role"),
    ("Role", "test-rbac-database-cleanup-role"),
    ("Role", "test-rbac-pod-log-reader-role"),
    ("RoleBinding", "test-rbac-pod-launcher-rolebinding"),
    ("RoleBinding", "test-rbac-pod-log-reader-rolebinding"),
    ("RoleBinding", "test-rbac-cleanup-rolebinding"),
    ("RoleBinding", "test-rbac-database-cleanup-rolebinding"),
]

SERVICE_ACCOUNT_NAME_TUPLES = [
    ("ServiceAccount", "test-rbac-cleanup"),
    ("ServiceAccount", "test-rbac-scheduler"),
    ("ServiceAccount", "test-rbac-worker"),
    ("ServiceAccount", "test-rbac-triggerer"),
    ("ServiceAccount", "test-rbac-pgbouncer"),
    ("ServiceAccount", "test-rbac-database-cleanup"),
    ("ServiceAccount", "test-rbac-flower"),
    ("ServiceAccount", "test-rbac-statsd"),
    ("ServiceAccount", "test-rbac-create-user-job"),
    ("ServiceAccount", "test-rbac-migrate-database-job"),
    ("ServiceAccount", "test-rbac-redis"),
]

CUSTOM_SERVICE_ACCOUNT_NAMES = (
    (CUSTOM_SCHEDULER_NAME := "TestScheduler"),
    (CUSTOM_DAG_PROCESSOR_NAME := "TestDagProcessor"),
    (CUSTOM_API_SERVER_NAME := "TestAPISserver"),
    (CUSTOM_WORKER_NAME := "TestWorker"),
    (CUSTOM_TRIGGERER_NAME := "TestTriggerer"),
    (CUSTOM_CLEANUP_NAME := "TestCleanup"),
    (CUSTOM_DATABASE_CLEANUP_NAME := "TestDatabaseCleanup"),
    (CUSTOM_FLOWER_NAME := "TestFlower"),
    (CUSTOM_PGBOUNCER_NAME := "TestPGBouncer"),
    (CUSTOM_STATSD_NAME := "TestStatsd"),
    (CUSTOM_CREATE_USER_JOBS_NAME := "TestCreateUserJob"),
    (CUSTOM_MIGRATE_DATABASE_JOBS_NAME := "TestMigrateDatabaseJob"),
    (CUSTOM_REDIS_NAME := "TestRedis"),
    (CUSTOM_POSTGRESQL_NAME := "TestPostgresql"),
)


class TestRBAC:
    """Tests RBAC."""

    def _get_object_tuples(self, sa: bool = True):
        tuples = copy(DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES)
        if sa:
            tuples.append(("ServiceAccount", "test-rbac-api-server"))
            tuples.append(("ServiceAccount", "test-rbac-dag-processor"))

        return tuples

    def test_deployments_no_rbac_no_sa(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
                "executor": "CeleryExecutor,KubernetesExecutor",
                "rbac": {"create": False},
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "create": False,
                    },
                },
                "databaseCleanup": {
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
                "dagProcessor": {"serviceAccount": {"create": False}},
                "apiServer": {"serviceAccount": {"create": False}},
                "workers": {"serviceAccount": {"create": False}},
                "triggerer": {"serviceAccount": {"create": False}},
                "statsd": {"serviceAccount": {"create": False}},
                "createUserJob": {"serviceAccount": {"create": False}},
                "migrateDatabaseJob": {"serviceAccount": {"create": False}},
                "flower": {"enabled": True, "serviceAccount": {"create": False}},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        assert sorted(list_of_kind_names_tuples) == sorted(self._get_object_tuples(sa=False))

    def test_deployments_no_rbac_with_sa(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
                "executor": "CeleryExecutor,KubernetesExecutor",
                "rbac": {"create": False},
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
                "flower": {"enabled": True},
                "pgbouncer": {"enabled": True},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = self._get_object_tuples() + SERVICE_ACCOUNT_NAME_TUPLES
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    def test_deployments_with_rbac_no_sa(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "create": False,
                    },
                },
                "databaseCleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "create": False,
                    },
                },
                "scheduler": {"serviceAccount": {"create": False}},
                "dagProcessor": {"serviceAccount": {"create": False}},
                "apiServer": {"serviceAccount": {"create": False}},
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
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = self._get_object_tuples(sa=False) + RBAC_ENABLED_KIND_NAME_TUPLES
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    def test_deployments_with_rbac_with_sa(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
                "flower": {"enabled": True},
                "pgbouncer": {"enabled": True},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = (
            self._get_object_tuples() + SERVICE_ACCOUNT_NAME_TUPLES + RBAC_ENABLED_KIND_NAME_TUPLES
        )
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    @pytest.mark.parametrize("executor", ["CeleryExecutor", "LocalExecutor"])
    def test_cleanup_resources_require_kubernetes_executor(self, executor):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
                "executor": executor,
                "rbac": {"create": True},
                "cleanup": {"enabled": True},
            },
            show_only=[
                "templates/rbac/pod-cleanup-role.yaml",
                "templates/rbac/pod-cleanup-rolebinding.yaml",
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/cleanup/cleanup-serviceaccount.yaml",
            ],
        )

        assert not k8s_objects

    def test_service_account_custom_names(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "fullnameOverride": "test-rbac",
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_CLEANUP_NAME,
                    },
                },
                "databaseCleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_DATABASE_CLEANUP_NAME,
                    },
                },
                "scheduler": {"serviceAccount": {"name": CUSTOM_SCHEDULER_NAME}},
                "dagProcessor": {"serviceAccount": {"name": CUSTOM_DAG_PROCESSOR_NAME}},
                "apiServer": {"serviceAccount": {"name": CUSTOM_API_SERVER_NAME}},
                "workers": {"serviceAccount": {"name": CUSTOM_WORKER_NAME}},
                "triggerer": {"serviceAccount": {"name": CUSTOM_TRIGGERER_NAME}},
                "flower": {"enabled": True, "serviceAccount": {"name": CUSTOM_FLOWER_NAME}},
                "statsd": {"serviceAccount": {"name": CUSTOM_STATSD_NAME}},
                "redis": {"serviceAccount": {"name": CUSTOM_REDIS_NAME}},
                "postgresql": {"serviceAccount": {"create": True, "name": CUSTOM_POSTGRESQL_NAME}},
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
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_CLEANUP_NAME,
                    },
                },
                "databaseCleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "name": CUSTOM_DATABASE_CLEANUP_NAME,
                    },
                },
                "scheduler": {"serviceAccount": {"name": CUSTOM_SCHEDULER_NAME}},
                "dagProcessor": {"serviceAccount": {"name": CUSTOM_DAG_PROCESSOR_NAME}},
                "apiServer": {"serviceAccount": {"name": CUSTOM_API_SERVER_NAME}},
                "workers": {"serviceAccount": {"name": CUSTOM_WORKER_NAME}},
                "triggerer": {"serviceAccount": {"name": CUSTOM_TRIGGERER_NAME}},
                "flower": {"enabled": True, "serviceAccount": {"name": CUSTOM_FLOWER_NAME}},
                "statsd": {"serviceAccount": {"name": CUSTOM_STATSD_NAME}},
                "redis": {"serviceAccount": {"name": CUSTOM_REDIS_NAME}},
                "postgresql": {"serviceAccount": {"name": CUSTOM_POSTGRESQL_NAME}},
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
            },
        )
        list_of_sa_names = [
            k8s_object["metadata"]["name"]
            for k8s_object in k8s_objects
            if k8s_object["kind"] == "ServiceAccount"
        ]
        service_account_names = [
            "test-rbac-scheduler",
            "test-rbac-dag-processor",
            "test-rbac-api-server",
            "test-rbac-triggerer",
            "test-rbac-migrate-database-job",
            "test-rbac-create-user-job",
        ]
        assert sorted(list_of_sa_names) == sorted(service_account_names)
