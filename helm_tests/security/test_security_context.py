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

from tests.charts.helm_template_generator import render_chart


class TestSCBackwardsCompatibility:
    """Tests SC Backward Compatibility."""

    def test_check_deployments_and_jobs(self):
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "webserver": {"defaultUser": {"enabled": True}},
                "flower": {"enabled": True},
                "airflowVersion": "2.2.0",
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
            assert 3000 == jmespath.search(
                "spec.template.spec.securityContext.runAsUser", doc
            )
            assert 30 == jmespath.search(
                "spec.template.spec.securityContext.fsGroup", doc
            )

    def test_check_statsd_uid(self):
        docs = render_chart(
            values={"statsd": {"enabled": True, "uid": 3000}},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert 3000 == jmespath.search(
            "spec.template.spec.securityContext.runAsUser", docs[0]
        )

    def test_check_pgbouncer_uid(self):
        docs = render_chart(
            values={"pgbouncer": {"enabled": True, "uid": 3000}},
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert 3000 == jmespath.search(
            "spec.template.spec.securityContext.runAsUser", docs[0]
        )

    def test_check_cleanup_job(self):
        docs = render_chart(
            values={"uid": 3000, "gid": 30, "cleanup": {"enabled": True}},
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert 3000 == jmespath.search(
            "spec.jobTemplate.spec.template.spec.securityContext.runAsUser", docs[0]
        )
        assert 30 == jmespath.search(
            "spec.jobTemplate.spec.template.spec.securityContext.fsGroup", docs[0]
        )

    def test_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={
                "dags": {"gitSync": {"enabled": True, "uid": 3000}},
                "airflowVersion": "1.10.15",
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
            ],
        )

        for doc in docs:
            assert "git-sync" in [
                c["name"] for c in jmespath.search("spec.template.spec.containers", doc)
            ]
            assert "git-sync-init" in [
                c["name"]
                for c in jmespath.search("spec.template.spec.initContainers", doc)
            ]
            assert 3000 == jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'].securityContext.runAsUser | [0]",
                doc,
            )
            assert 3000 == jmespath.search(
                "spec.template.spec.containers[?name=='git-sync'].securityContext.runAsUser | [0]",
                doc,
            )


class TestSecurityContext:
    """Tests security context."""

    # Test securityContext setting for Pods and Containers
    def test_check_default_setting(self):
        docs = render_chart(
            values={
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "webserver": {"defaultUser": {"enabled": True}},
                "flower": {"enabled": True},
                "statsd": {"enabled": False},
                "airflowVersion": "2.2.0",
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
            assert 6000 == jmespath.search(
                "spec.template.spec.securityContext.runAsUser", doc
            )
            assert 60 == jmespath.search(
                "spec.template.spec.securityContext.fsGroup", doc
            )

    # Test priority:
    # <local>.securityContext > securityContext > uid + gid
    def test_check_local_setting(self):
        component_contexts = {"securityContext": {"runAsUser": 9000, "fsGroup": 90}}
        docs = render_chart(
            values={
                "uid": 3000,
                "gid": 30,
                "securityContext": {"runAsUser": 6000, "fsGroup": 60},
                "webserver": {"defaultUser": {"enabled": True}, **component_contexts},
                "workers": {**component_contexts},
                "flower": {"enabled": True, **component_contexts},
                "scheduler": {**component_contexts},
                "createUserJob": {**component_contexts},
                "migrateDatabaseJob": {**component_contexts},
                "triggerer": {**component_contexts},
                "redis": {**component_contexts},
                "statsd": {"enabled": True, **component_contexts},
                "airflowVersion": "2.2.0",
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
            assert 9000 == jmespath.search(
                "spec.template.spec.securityContext.runAsUser", doc
            )
            assert 90 == jmespath.search(
                "spec.template.spec.securityContext.fsGroup", doc
            )

    # Test containerSecurity priority over uid under components using localSecurityContext
    def test_check_local_uid(self):
        component_contexts = {"uid": 3000, "securityContext": {"runAsUser": 7000}}
        docs = render_chart(
            values={
                "redis": {**component_contexts},
                "statsd": {"enabled": True, **component_contexts},
            },
            show_only=[
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            ],
        )

        for doc in docs:
            assert 7000 == jmespath.search(
                "spec.template.spec.securityContext.runAsUser", doc
            )

    # Test containerSecurity priority over uid under dags.gitSync
    def test_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "uid": 9000,
                        "securityContext": {"runAsUser": 8000},
                    }
                },
                "airflowVersion": "1.10.15",
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
            ],
        )

        for doc in docs:
            assert "git-sync" in [
                c["name"] for c in jmespath.search("spec.template.spec.containers", doc)
            ]
            assert "git-sync-init" in [
                c["name"]
                for c in jmespath.search("spec.template.spec.initContainers", doc)
            ]
            assert 8000 == jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'].securityContext.runAsUser | [0]",
                doc,
            )
            assert 8000 == jmespath.search(
                "spec.template.spec.containers[?name=='git-sync'].securityContext.runAsUser | [0]",
                doc,
            )

    # Test securityContexts for main containers
    def test_global_security_context(self):
        ctx_value_pod = {"runAsUser": 7000}
        ctx_value_container = {"allowPrivilegeEscalation": False}
        docs = render_chart(
            values={
                "securityContexts": {
                    "containers": ctx_value_container,
                    "pod": ctx_value_pod,
                },
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
            assert ctx_value_pod == jmespath.search(
                "spec.template.spec.securityContext", doc
            )

        # Global security context is not propagated to pgbouncer, redis and statsd, so we test default value
        default_ctx_value_container = {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
        }
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
        assert default_ctx_value_pod_statsd == jmespath.search(
            "spec.template.spec.securityContext", docs[-2]
        )
        assert default_ctx_value_pod_redis == jmespath.search(
            "spec.template.spec.securityContext", docs[-1]
        )

    # Test securityContexts for main containers
    def test_main_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        security_context = {"securityContexts": {"container": ctx_value}}
        docs = render_chart(
            values={
                "cleanup": {"enabled": True, **security_context},
                "scheduler": {**security_context},
                "webserver": {**security_context},
                "workers": {**security_context},
                "flower": {"enabled": True, **security_context},
                "statsd": {**security_context},
                "createUserJob": {**security_context},
                "migrateDatabaseJob": {**security_context},
                "triggerer": {**security_context},
                "pgbouncer": {"enabled": True, **security_context},
                "redis": {**security_context},
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
            assert ctx_value == jmespath.search(
                "spec.template.spec.containers[0].securityContext", doc
            )

    # Test securityContexts for log-groomer-sidecar main container
    def test_log_groomer_sidecar_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        spec = {"logGroomerSidecar": {"securityContexts": {"container": ctx_value}}}
        docs = render_chart(
            values={
                "scheduler": {**spec},
                "workers": {**spec},
                "dagProcessor": {**spec},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert ctx_value == jmespath.search(
                "spec.template.spec.containers[1].securityContext", doc
            )

    # Test securityContexts for metrics-explorer main container
    def test_metrics_explorer_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        docs = render_chart(
            values={
                "pgbouncer": {
                    "enabled": True,
                    "metricsExporterSidecar": {
                        "securityContexts": {"container": ctx_value}
                    },
                },
            },
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert ctx_value == jmespath.search(
            "spec.template.spec.containers[1].securityContext", docs[0]
        )

    # Test securityContexts for worker-kerberos main container
    def test_worker_kerberos_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        docs = render_chart(
            values={
                "workers": {
                    "kerberosSidecar": {
                        "enabled": True,
                        "securityContexts": {"container": ctx_value},
                    }
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert ctx_value == jmespath.search(
            "spec.template.spec.containers[2].securityContext", docs[0]
        )

    # Test securityContexts for the wait-for-migrations init containers
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
                "scheduler": {**spec},
                "webserver": {**spec},
                "triggerer": {**spec},
                "workers": {
                    "waitForMigrations": {"securityContexts": {"container": ctx_value}}
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        for doc in docs:
            assert ctx_value == jmespath.search(
                "spec.template.spec.initContainers[0].securityContext", doc
            )

    # Test securityContexts for volume-permissions init container
    def test_volume_permissions_init_container_setting(self):
        ctx_value = {"allowPrivilegeEscalation": False}
        docs = render_chart(
            values={
                "workers": {
                    "persistence": {
                        "enabled": True,
                        "fixPermissions": True,
                        "securityContexts": {"container": ctx_value},
                    }
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert ctx_value == jmespath.search(
            "spec.template.spec.initContainers[0].securityContext", docs[0]
        )

    # Test securityContexts for main pods
    def test_main_pod_setting(self):
        ctx_value = {"runAsUser": 7000}
        security_context = {"securityContexts": {"pod": ctx_value}}
        docs = render_chart(
            values={
                "cleanup": {"enabled": True, **security_context},
                "scheduler": {**security_context},
                "webserver": {**security_context},
                "workers": {**security_context},
                "flower": {"enabled": True, **security_context},
                "statsd": {**security_context},
                "createUserJob": {**security_context},
                "migrateDatabaseJob": {**security_context},
                "triggerer": {**security_context},
                "pgbouncer": {"enabled": True, **security_context},
                "redis": {**security_context},
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
            "spec.jobTemplate.spec.template.spec.securityContext", docs[0]
        )

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.securityContext", doc)

    # Test securityContexts for main pods
    def test_main_pod_setting_legacy_security(self):
        ctx_value = {"runAsUser": 7000}
        security_context = {"securityContext": ctx_value}
        docs = render_chart(
            values={
                "cleanup": {"enabled": True, **security_context},
                "scheduler": {**security_context},
                "webserver": {**security_context},
                "workers": {**security_context},
                "flower": {"enabled": True, **security_context},
                "statsd": {**security_context},
                "createUserJob": {**security_context},
                "migrateDatabaseJob": {**security_context},
                "triggerer": {**security_context},
                "redis": {**security_context},
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

        assert ctx_value == jmespath.search(
            "spec.jobTemplate.spec.template.spec.securityContext", docs[0]
        )

        for doc in docs[1:]:
            assert ctx_value == jmespath.search("spec.template.spec.securityContext", doc)
