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
from chart_utils.helm_template_generator import render_chart


class TestSCBackwardsCompatibility:
    """Tests SC Backward Compatibility."""

    @pytest.mark.parametrize(
        "executor",
        [
            "CeleryExecutor",
            "CeleryKubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
            "KubernetesExecutor",
        ],
    )
    def test_uid_gid_set_for_airflow_2(self, executor):
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "flower": {"enabled": True},
                "airflowVersion": "2.11.0",
                "executor": executor,
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 3000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 30

    @pytest.mark.parametrize("executor", ["CeleryExecutor", "KubernetesExecutor"])
    def test_uid_gid_set(self, executor):
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "flower": {"enabled": True},
                "executor": executor,
            },
            show_only=[
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 3000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 30

    def test_check_statsd_uid(self):
        docs = render_chart(
            values={"statsd": {"enabled": True, "uid": 3000}},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0]) == 3000

    def test_check_pgbouncer_uid(self):
        docs = render_chart(
            values={"pgbouncer": {"enabled": True, "uid": 3000}},
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0]) == 3000

    @pytest.mark.parametrize(
        "executor", ["CeleryKubernetesExecutor", "CeleryExecutor,KubernetesExecutor", "KubernetesExecutor"]
    )
    def test_check_cleanup_job(self, executor):
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "executor": executor,
                "cleanup": {"enabled": True},
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert (
            jmespath.search("spec.jobTemplate.spec.template.spec.securityContext.runAsUser", docs[0]) == 3000
        )
        assert jmespath.search("spec.jobTemplate.spec.template.spec.securityContext.fsGroup", docs[0]) == 30

    def test_gitsync_sidecar_and_init_container_airflow_2(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True, "uid": 3000}}, "airflowVersion": "2.11.0"},
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
            ],
        )

        for doc in docs:
            assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", doc)]
            assert "git-sync-init" in [
                c["name"] for c in jmespath.search("spec.template.spec.initContainers", doc)
            ]
            assert (
                jmespath.search(
                    "spec.template.spec.initContainers[?name=='git-sync-init'].securityContext.runAsUser | [0]",
                    doc,
                )
                == 3000
            )
            assert (
                jmespath.search(
                    "spec.template.spec.containers[?name=='git-sync'].securityContext.runAsUser | [0]",
                    doc,
                )
                == 3000
            )

    def test_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={
                "dags": {"gitSync": {"enabled": True, "uid": 3000}},
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", doc)]
            assert "git-sync-init" in [
                c["name"] for c in jmespath.search("spec.template.spec.initContainers", doc)
            ]
            assert (
                jmespath.search(
                    "spec.template.spec.initContainers[?name=='git-sync-init'].securityContext.runAsUser | [0]",
                    doc,
                )
                == 3000
            )
            assert (
                jmespath.search(
                    "spec.template.spec.containers[?name=='git-sync'].securityContext.runAsUser | [0]",
                    doc,
                )
                == 3000
            )


class TestSecurityContext:
    """Tests security context."""

    # Test securityContext setting for Pods and Containers
    def test_default_setting_airflow_2(self):
        docs = render_chart(
            values={
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "flower": {"enabled": True},
                "statsd": {"enabled": False},
                "airflowVersion": "2.11.0",
                "executor": "CeleryKubernetesExecutor",
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 6000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 60

    def test_default_setting(self):
        docs = render_chart(
            values={
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "flower": {"enabled": True},
                "statsd": {"enabled": False},
            },
            show_only=[
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 6000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 60

    # Test priority:
    # <local>.securityContext > securityContext > uid + gid
    def test_check_local_setting_airflow_2(self):
        component_contexts = {"securityContext": {"runAsUser": 9000, "fsGroup": 90}}
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "webserver": component_contexts,
                "workers": component_contexts,
                "flower": {"enabled": True, **component_contexts},
                "scheduler": component_contexts,
                "createUserJob": component_contexts,
                "migrateDatabaseJob": component_contexts,
                "triggerer": component_contexts,
                "redis": component_contexts,
                "statsd": {"enabled": True, **component_contexts},
                "airflowVersion": "2.11.0",
                "executor": "CeleryKubernetesExecutor",
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 9000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 90

    def test_check_local_setting(self):
        component_contexts = {"securityContext": {"runAsUser": 9000, "fsGroup": 90}}
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "workers": component_contexts,
                "flower": {"enabled": True, **component_contexts},
                "scheduler": component_contexts,
                "dagProcessor": component_contexts,
                "createUserJob": component_contexts,
                "migrateDatabaseJob": component_contexts,
                "triggerer": component_contexts,
                "redis": component_contexts,
                "statsd": {"enabled": True, **component_contexts},
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 9000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 90

    # Test priority:
    # <local>.securityContexts > securityContexts > uid + gid
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}},
            {"celery": {"securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}}},
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {"name": "test", "securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}}
                    ],
                },
            },
        ],
    )
    def test_check_local_setting_default_version(self, workers_values):
        component_contexts = {"securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}}
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "securityContexts": {"pod": {"runAsUser": 6000, "fsGroup": 60}},
                "apiServer": component_contexts,
                "workers": workers_values,
                "dagProcessor": component_contexts,
                "flower": {"enabled": True, **component_contexts},
                "scheduler": component_contexts,
                "createUserJob": component_contexts,
                "migrateDatabaseJob": component_contexts,
                "triggerer": component_contexts,
                "redis": component_contexts,
                "statsd": {"enabled": True, **component_contexts},
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 9000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 90

    # Test containerSecurity priority over uid under components using localSecurityContext
    def test_check_local_uid(self):
        component_contexts = {"uid": 3000, "securityContext": {"runAsUser": 7000}}
        docs = render_chart(
            values={
                "redis": component_contexts,
                "statsd": {"enabled": True, **component_contexts},
            },
            show_only=[
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 7000

    # Test securityContexts for main containers
    def test_global_security_context(self):
        ctx_value_pod = {"runAsUser": 7000}
        ctx_value_container = {"allowPrivilegeEscalation": False}
        docs = render_chart(
            values={
                "securityContexts": {"containers": ctx_value_container, "pod": ctx_value_pod},
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True},
                "flower": {"enabled": True},
                "pgbouncer": {"enabled": True},
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        assert ctx_value_container == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].securityContext", docs[0]
        )
        assert ctx_value_pod == jmespath.search(
            "spec.jobTemplate.spec.template.spec.securityContext", docs[0]
        )

        for doc in docs[1:-3]:
            assert ctx_value_container == jmespath.search(
                "spec.template.spec.containers[0].securityContext", doc
            )
            assert ctx_value_pod == jmespath.search("spec.template.spec.securityContext", doc)

        # Global security context is not propagated to pgbouncer, redis and statsd, so we test default value
        default_ctx_value_container = {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}
        default_ctx_value_pod_pgbouncer = {"runAsUser": 65534}
        default_ctx_value_pod_statsd = {"runAsUser": 65534}
        default_ctx_value_pod_redis = {"runAsUser": 0}
        for doc in docs[-3:]:
            assert default_ctx_value_container == jmespath.search(
                "spec.template.spec.containers[0].securityContext", doc
            )
        # Test pgbouncer metrics-exporter container
        assert default_ctx_value_container == jmespath.search(
            "spec.template.spec.containers[1].securityContext", docs[-3]
        )
        assert default_ctx_value_pod_pgbouncer == jmespath.search(
            "spec.template.spec.securityContext", docs[-3]
        )
        assert default_ctx_value_pod_statsd == jmespath.search("spec.template.spec.securityContext", docs[-2])
        assert default_ctx_value_pod_redis == jmespath.search("spec.template.spec.securityContext", docs[-1])

    # Test securityContexts for main containers
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}},
            {"celery": {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}}},
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                        }
                    ],
                },
            },
        ],
    )
    def test_main_container_setting_airflow_2(self, workers_values):
        ctx_value = {"allowPrivilegeEscalation": False}
        security_context = {"securityContexts": {"container": ctx_value}}
        docs = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True, **security_context},
                "scheduler": security_context,
                "webserver": security_context,
                "workers": workers_values,
                "flower": {"enabled": True, **security_context},
                "statsd": security_context,
                "createUserJob": security_context,
                "migrateDatabaseJob": security_context,
                "triggerer": security_context,
                "pgbouncer": {"enabled": True, **security_context},
                "redis": security_context,
                "airflowVersion": "2.11.0",
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        assert ctx_value == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].securityContext", docs[0]
        )

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.containers[0].securityContext", doc)

    # Test securityContexts for main containers
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}},
            {"celery": {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}}},
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                        }
                    ],
                },
            },
        ],
    )
    def test_main_container_setting(self, workers_values):
        ctx_value = {"allowPrivilegeEscalation": False}
        security_context = {"securityContexts": {"container": ctx_value}}
        docs = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True, **security_context},
                "scheduler": security_context,
                "dagProcessor": security_context,
                "apiServer": security_context,
                "workers": workers_values,
                "flower": {"enabled": True, **security_context},
                "statsd": security_context,
                "createUserJob": security_context,
                "migrateDatabaseJob": security_context,
                "triggerer": security_context,
                "pgbouncer": {"enabled": True, **security_context},
                "redis": security_context,
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        assert ctx_value == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].securityContext", docs[0]
        )

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.containers[0].securityContext", doc)

    # Test securityContexts for log-groomer-sidecar main container
    def test_log_groomer_sidecar_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        spec = {"logGroomerSidecar": {"securityContexts": {"container": ctx_value}}}
        docs = render_chart(
            values={
                "scheduler": spec,
                "workers": spec,
                "dagProcessor": spec,
                "triggerer": spec,
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert ctx_value == jmespath.search("spec.template.spec.containers[1].securityContext", doc)

    # Test securityContexts for metrics-explorer main container
    def test_metrics_explorer_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        docs = render_chart(
            values={
                "pgbouncer": {
                    "enabled": True,
                    "metricsExporterSidecar": {"securityContexts": {"container": ctx_value}},
                },
            },
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert ctx_value == jmespath.search("spec.template.spec.containers[1].securityContext", docs[0])

    # Test securityContexts for worker-kerberos main container
    def test_worker_kerberos_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        docs = render_chart(
            values={
                "workers": {
                    "kerberosSidecar": {"enabled": True, "securityContexts": {"container": ctx_value}}
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert ctx_value == jmespath.search("spec.template.spec.containers[2].securityContext", docs[0])

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "kerberosInitContainer": {
                    "enabled": True,
                    "securityContexts": {"container": {"runAsUser": 2000}},
                }
            },
            {
                "celery": {
                    "kerberosInitContainer": {
                        "enabled": True,
                        "securityContexts": {"container": {"runAsUser": 2000}},
                    }
                }
            },
            {
                "kerberosInitContainer": {
                    "enabled": True,
                    "securityContexts": {"container": {"runAsUser": 1000}},
                },
                "celery": {
                    "kerberosInitContainer": {
                        "enabled": True,
                        "securityContexts": {"container": {"runAsUser": 2000}},
                    }
                },
            },
        ],
    )
    def test_worker_kerberos_init_container_security_context(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.initContainers[?name=='kerberos-init'] | [0].securityContext", docs[0]
        ) == {"runAsUser": 2000}

    # Test securityContexts for the wait-for-migrations init containers
    def test_wait_for_migrations_init_container_setting_airflow_2(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        spec = {
            "waitForMigrations": {
                "enabled": True,
                "securityContexts": {"container": ctx_value},
            }
        }
        docs = render_chart(
            values={
                "scheduler": spec,
                "webserver": spec,
                "triggerer": spec,
                "workers": {"waitForMigrations": {"securityContexts": {"container": ctx_value}}},
                "airflowVersion": "2.11.0",
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        for doc in docs:
            assert ctx_value == jmespath.search("spec.template.spec.initContainers[0].securityContext", doc)

    def test_wait_for_migrations_init_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        spec = {
            "waitForMigrations": {
                "enabled": True,
                "securityContexts": {"container": ctx_value},
            }
        }
        docs = render_chart(
            values={
                "scheduler": spec,
                "apiServer": spec,
                "triggerer": spec,
                "dagProcessor": spec,
                "workers": {"waitForMigrations": {"securityContexts": {"container": ctx_value}}},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert ctx_value == jmespath.search("spec.template.spec.initContainers[0].securityContext", doc)

    # Test securityContexts for volume-permissions init container
    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "persistence": {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}},
                "celery": {"persistence": {"enabled": True, "fixPermissions": True}},
            },
            {
                "celery": {
                    "persistence": {
                        "enabled": True,
                        "fixPermissions": True,
                        "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                    }
                }
            },
            {
                "persistence": {"securityContexts": {"container": {"allowPrivilegeEscalation": True}}},
                "celery": {
                    "persistence": {
                        "enabled": True,
                        "fixPermissions": True,
                        "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                    }
                },
            },
        ],
    )
    def test_volume_permissions_init_container_setting(self, workers_values):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False
        }

    # Test securityContexts for main pods
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"securityContexts": {"pod": {"runAsUser": 7000}}},
            {"celery": {"securityContexts": {"pod": {"runAsUser": 7000}}}},
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "test", "securityContexts": {"pod": {"runAsUser": 7000}}}],
                },
            },
        ],
    )
    def test_main_pod_setting_airflow_2(self, workers_values):
        ctx_value = {"runAsUser": 7000}
        security_context = {"securityContexts": {"pod": ctx_value}}
        docs = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True, **security_context},
                "scheduler": security_context,
                "webserver": security_context,
                "workers": workers_values,
                "flower": {"enabled": True, **security_context},
                "statsd": security_context,
                "createUserJob": security_context,
                "migrateDatabaseJob": security_context,
                "triggerer": security_context,
                "pgbouncer": {"enabled": True, **security_context},
                "redis": security_context,
                "airflowVersion": "2.11.0",
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        assert ctx_value == jmespath.search("spec.jobTemplate.spec.template.spec.securityContext", docs[0])

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.securityContext", doc)

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"securityContexts": {"pod": {"runAsUser": 7000}}},
            {"celery": {"securityContexts": {"pod": {"runAsUser": 7000}}}},
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "test", "securityContexts": {"pod": {"runAsUser": 7000}}}],
                },
            },
        ],
    )
    def test_main_pod_setting(self, workers_values):
        ctx_value = {"runAsUser": 7000}
        security_context = {"securityContexts": {"pod": ctx_value}}
        docs = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True, **security_context},
                "scheduler": security_context,
                "apiServer": security_context,
                "workers": workers_values,
                "flower": {"enabled": True, **security_context},
                "statsd": security_context,
                "createUserJob": security_context,
                "migrateDatabaseJob": security_context,
                "triggerer": security_context,
                "pgbouncer": {"enabled": True, **security_context},
                "redis": security_context,
                "dagProcessor": security_context,
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        assert ctx_value == jmespath.search("spec.jobTemplate.spec.template.spec.securityContext", docs[0])

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.securityContext", doc)

    # Test securityContexts for main pods
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"securityContext": {"runAsUser": 7000}},
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "test", "securityContext": {"runAsUser": 7000}}],
                },
            },
        ],
    )
    def test_main_pod_setting_legacy_security_airflow_2(self, workers_values):
        ctx_value = {"runAsUser": 7000}
        security_context = {"securityContext": ctx_value}
        docs = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True, **security_context},
                "scheduler": security_context,
                "webserver": security_context,
                "workers": workers_values,
                "flower": {"enabled": True, **security_context},
                "statsd": security_context,
                "createUserJob": security_context,
                "migrateDatabaseJob": security_context,
                "triggerer": security_context,
                "redis": security_context,
                "airflowVersion": "2.11.0",
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        assert ctx_value == jmespath.search("spec.jobTemplate.spec.template.spec.securityContext", docs[0])

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.securityContext", doc)

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"securityContext": {"runAsUser": 7000}},
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "test", "securityContext": {"runAsUser": 7000}}],
                },
            },
        ],
    )
    def test_main_pod_setting_legacy_security(self, workers_values):
        ctx_value = {"runAsUser": 7000}
        security_context = {"securityContext": ctx_value}
        docs = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True, **security_context},
                "scheduler": security_context,
                "workers": workers_values,
                "flower": {"enabled": True, **security_context},
                "statsd": security_context,
                "createUserJob": security_context,
                "migrateDatabaseJob": security_context,
                "triggerer": security_context,
                "redis": security_context,
                "dagProcessor": security_context,
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        assert ctx_value == jmespath.search("spec.jobTemplate.spec.template.spec.securityContext", docs[0])

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.securityContext", doc)

    def test_deprecated_overwrite_global(self):
        docs = render_chart(
            values={
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}},
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 9000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 90

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}},
            },
            {
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "celery": {"securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}},
            },
            {
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {"name": "test", "securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}}
                    ],
                },
            },
        ],
    )
    def test_deprecated_overwrite_local(self, workers_values):
        context = {
            "securityContext": {"runAsUser": 6000, "fsGroup": 60},
            "securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}},
        }

        docs = render_chart(
            values={
                "flower": context,
                "scheduler": context,
                "triggerer": context,
                "workers": workers_values,
                "dagProcessor": context,
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.securityContext.runAsUser", doc) == 9000
            assert jmespath.search("spec.template.spec.securityContext.fsGroup", doc) == 90

    def test_workers_overwrite_local(self):
        docs = render_chart(
            values={
                "workers": {
                    "securityContexts": {"pod": {"runAsUser": 6000, "fsGroup": 60}},
                    "celery": {"securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0]) == 9000
        assert jmespath.search("spec.template.spec.securityContext.fsGroup", docs[0]) == 90

    @pytest.mark.parametrize(
        "values",
        [
            {
                "securityContexts": {"pod": {"runAsUser": 6000, "fsGroup": 60}},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {"name": "test", "securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}}
                    ],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "securityContexts": {"pod": {"runAsUser": 6000, "fsGroup": 60}},
                    "sets": [
                        {"name": "test", "securityContexts": {"pod": {"runAsUser": 9000, "fsGroup": 90}}}
                    ],
                },
            },
        ],
    )
    def test_workers_sets_overwrite_local(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0]) == 9000
        assert jmespath.search("spec.template.spec.securityContext.fsGroup", docs[0]) == 90

    @pytest.mark.parametrize(
        "values",
        [
            {
                "securityContexts": {"container": {"allowPrivilegeEscalation": True}},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                        }
                    ],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "securityContexts": {"container": {"allowPrivilegeEscalation": True}},
                    "sets": [
                        {
                            "name": "test",
                            "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                        }
                    ],
                },
            },
        ],
    )
    def test_workers_sets_overwrite_local_container(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False
        }

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {
                                "fixPermissions": True,
                                "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                            },
                        }
                    ],
                }
            },
            {
                "persistence": {"securityContexts": {"container": {"allowPrivilegeEscalation": True}}},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {
                                "fixPermissions": True,
                                "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                            },
                        }
                    ],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"securityContexts": {"container": {"allowPrivilegeEscalation": True}}},
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {
                                "fixPermissions": True,
                                "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                            },
                        }
                    ],
                }
            },
        ],
    )
    def test_workers_sets_volume_permissions_init_container_setting(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False
        }
