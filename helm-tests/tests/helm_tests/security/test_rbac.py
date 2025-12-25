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
from packaging.version import parse as parse_version

DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES = [
    ("Secret", "test-rbac-postgresql"),
    ("Secret", "test-rbac-metadata"),
    ("Secret", "test-rbac-pgbouncer-config"),
    ("Secret", "test-rbac-pgbouncer-stats"),
    ("ConfigMap", "test-rbac-config"),
    ("ConfigMap", "test-rbac-statsd"),
    ("Service", "test-rbac-postgresql-hl"),
    ("Service", "test-rbac-postgresql"),
    ("Service", "test-rbac-statsd"),
    ("Service", "test-rbac-flower"),
    ("Service", "test-rbac-pgbouncer"),
    ("Service", "test-rbac-redis"),
    ("Service", "test-rbac-worker"),
    ("Deployment", "test-rbac-scheduler"),
    ("Deployment", "test-rbac-statsd"),
    ("Deployment", "test-rbac-flower"),
    ("Deployment", "test-rbac-pgbouncer"),
    ("StatefulSet", "test-rbac-postgresql"),
    ("StatefulSet", "test-rbac-redis"),
    ("StatefulSet", "test-rbac-worker"),
    ("Secret", "test-rbac-broker-url"),
    ("Secret", "test-rbac-fernet-key"),
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
CUSTOM_WEBSERVER_NAME = "TestWebserver"
CUSTOM_WORKER_CELERY_NAME = "TestWorkerCelery"
CUSTOM_WORKER_KUBERNETES_NAME = "TestWorkerKubernetes"

parametrize_version = pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "3.0.0", "default"])


class TestRBAC:
    """Tests RBAC."""

    def _get_values_with_version(self, values, version):
        if version != "default":
            values["airflowVersion"] = version
        return values

    def _is_airflow_3_or_above(self, version):
        return version == "default" or (parse_version(version) >= parse_version("3.0.0"))

    def _get_object_tuples(self, version, sa: bool = True, dedicated_workers_sa: None | bool = None):
        tuples = copy(DEPLOYMENT_NO_RBAC_NO_SA_KIND_NAME_TUPLES)
        if version in {"default", "3.0.0"}:
            tuples.append(("Service", "test-rbac-triggerer"))
            tuples.append(("StatefulSet", "test-rbac-triggerer"))
        else:
            tuples.append(("Deployment", "test-rbac-triggerer"))
        if version == "2.3.2":
            tuples.append(("Secret", "test-rbac-result-backend"))
        if self._is_airflow_3_or_above(version):
            tuples.extend(
                (
                    ("Service", "test-rbac-api-server"),
                    ("Deployment", "test-rbac-api-server"),
                    ("Deployment", "test-rbac-dag-processor"),
                    ("Secret", "test-rbac-api-secret-key"),
                    ("Secret", "test-rbac-jwt-secret"),
                )
            )
            if sa:
                tuples.append(("ServiceAccount", "test-rbac-api-server"))
                tuples.append(("ServiceAccount", "test-rbac-dag-processor"))
        else:
            tuples.extend(
                (
                    ("Service", "test-rbac-webserver"),
                    ("Deployment", "test-rbac-webserver"),
                    ("Secret", "test-rbac-webserver-secret-key"),
                )
            )
            if sa:
                tuples.append(("ServiceAccount", "test-rbac-webserver"))

        if dedicated_workers_sa is not None:
            if dedicated_workers_sa:
                tuples.append(("ServiceAccount", "test-rbac-worker-celery"))
                tuples.append(("ServiceAccount", "test-rbac-worker-kubernetes"))
            else:
                tuples.append(("ServiceAccount", "test-rbac-worker"))

        return tuples

    @parametrize_version
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"serviceAccount": {"create": False}},
            {
                "useWorkerDedicatedServiceAccounts": True,
                "celery": {"serviceAccount": {"create": False}},
                "kubernetes": {"serviceAccount": {"create": False}},
            },
        ],
    )
    def test_deployments_no_rbac_no_sa(self, version, workers_values):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
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
                    "webserver": {"serviceAccount": {"create": False}},
                    "apiServer": {"serviceAccount": {"create": False}},
                    "workers": workers_values,
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
        assert sorted(list_of_kind_names_tuples) == sorted(self._get_object_tuples(version, sa=False))

    @parametrize_version
    @pytest.mark.parametrize("dedicated_workers_sa", [False, True])
    def test_deployments_no_rbac_with_sa(self, version, dedicated_workers_sa):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "test-rbac",
                    "executor": "CeleryExecutor,KubernetesExecutor",
                    "rbac": {"create": False},
                    "cleanup": {"enabled": True},
                    "databaseCleanup": {"enabled": True},
                    "flower": {"enabled": True},
                    "pgbouncer": {"enabled": True},
                    "workers": {"useWorkerDedicatedServiceAccounts": dedicated_workers_sa},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = (
            self._get_object_tuples(version, dedicated_workers_sa=dedicated_workers_sa)
            + SERVICE_ACCOUNT_NAME_TUPLES
        )
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    @parametrize_version
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"serviceAccount": {"create": False}},
            {
                "useWorkerDedicatedServiceAccounts": True,
                "celery": {"serviceAccount": {"create": False}},
                "kubernetes": {"serviceAccount": {"create": False}},
            },
        ],
    )
    def test_deployments_with_rbac_no_sa(self, version, workers_values):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
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
                    "webserver": {"serviceAccount": {"create": False}},
                    "apiServer": {"serviceAccount": {"create": False}},
                    "workers": workers_values,
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
        real_list_of_kind_names = self._get_object_tuples(version, sa=False) + RBAC_ENABLED_KIND_NAME_TUPLES
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    @parametrize_version
    @pytest.mark.parametrize("dedicated_workers_sa", [False, True])
    def test_deployments_with_rbac_with_sa(self, version, dedicated_workers_sa):
        k8s_objects = render_chart(
            "test-rbac",
            values=self._get_values_with_version(
                values={
                    "fullnameOverride": "test-rbac",
                    "executor": "CeleryExecutor,KubernetesExecutor",
                    "cleanup": {"enabled": True},
                    "databaseCleanup": {"enabled": True},
                    "flower": {"enabled": True},
                    "pgbouncer": {"enabled": True},
                    "workers": {"useWorkerDedicatedServiceAccounts": dedicated_workers_sa},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        real_list_of_kind_names = (
            self._get_object_tuples(version, dedicated_workers_sa=dedicated_workers_sa)
            + SERVICE_ACCOUNT_NAME_TUPLES
            + RBAC_ENABLED_KIND_NAME_TUPLES
        )
        assert sorted(list_of_kind_names_tuples) == sorted(real_list_of_kind_names)

    def test_service_account_custom_names(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "airflowVersion": "3.0.0",
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

    def test_workers_service_account_custom_name(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "airflowVersion": "3.0.0",
                "fullnameOverride": "test-rbac",
                "executor": "CeleryExecutor,KubernetesExecutor",
                "workers": {
                    "useWorkerDedicatedServiceAccounts": True,
                    "celery": {"serviceAccount": {"name": CUSTOM_WORKER_CELERY_NAME}},
                    "kubernetes": {"serviceAccount": {"name": CUSTOM_WORKER_KUBERNETES_NAME}},
                },
            },
            show_only=[
                "templates/workers/worker-celery-serviceaccount.yaml",
                "templates/workers/worker-kubernetes-serviceaccount.yaml",
            ],
        )

        list_of_sa_names = [
            k8s_object["metadata"]["name"]
            for k8s_object in k8s_objects
            if k8s_object["kind"] == "ServiceAccount"
        ]
        assert len(k8s_objects) == 2
        assert sorted(list_of_sa_names) == [CUSTOM_WORKER_CELERY_NAME, CUSTOM_WORKER_KUBERNETES_NAME]

    def test_webserver_service_account_name_airflow_2(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "airflowVersion": "2.10.5",
                "fullnameOverride": "test-rbac",
                "webserver": {"serviceAccount": {"name": CUSTOM_WEBSERVER_NAME}},
            },
            show_only=["templates/webserver/webserver-serviceaccount.yaml"],
        )
        sa_name = jmespath.search("metadata.name", k8s_objects[0])
        assert sa_name == CUSTOM_WEBSERVER_NAME

    def test_service_account_custom_names_in_objects(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "airflowVersion": "3.0.0",
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

    def test_workers_celery_service_account_custom_names_in_objects(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "airflowVersion": "3.0.0",
                "fullnameOverride": "test-rbac",
                "workers": {
                    "useWorkerDedicatedServiceAccounts": True,
                    "celery": {"serviceAccount": {"name": CUSTOM_WORKER_CELERY_NAME}},
                },
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
            ],
        )

        assert (
            jmespath.search("spec.template.spec.serviceAccountName", k8s_objects[0])
            == CUSTOM_WORKER_CELERY_NAME
        )

    def test_service_account_without_resource(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "airflowVersion": "3.0.0",
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
