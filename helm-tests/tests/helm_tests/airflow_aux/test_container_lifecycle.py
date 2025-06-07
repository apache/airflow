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

HOOK_TYPE = "preStop"
LIFECYCLE_PARSED = {"exec": {"command": ["echo", HOOK_TYPE, "release-name"]}}
LIFECYCLE_CONFIG = {HOOK_TYPE: {"exec": {"command": ["echo", HOOK_TYPE, "{{ .Release.Name }}"]}}}


class TestContainerLifecycleHooks:
    """Tests container lifecycle hooks."""

    # Test container lifecycle hooks default setting
    def test_check_default_setting(self):
        docs = render_chart(
            values={
                "webserver": {"defaultUser": {"enabled": True}},
                "flower": {"enabled": True},
                "statsd": {"enabled": True},
                "pgbouncer": {"enabled": True},
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
            ],
        )

        # Default for every service is None except for pgbouncer
        for doc in docs[:-1]:
            assert jmespath.search("spec.template.spec.containers[0].lifecycle", doc) is None

        pgbouncer_default_value = {
            "exec": {"command": ["/bin/sh", "-c", "killall -INT pgbouncer && sleep 120"]}
        }
        assert pgbouncer_default_value == jmespath.search(
            "spec.template.spec.containers[0].lifecycle.preStop", docs[-1]
        )
        assert jmespath.search("spec.template.spec.containers[0].lifecycle.postStart", docs[-1]) is None

    # Test Global container lifecycle hooks for the main services
    def test_global_setting(self):
        docs = render_chart(
            values={
                "containerLifecycleHooks": LIFECYCLE_CONFIG,
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
            ],
        )

        for doc in docs:
            assert (
                jmespath.search(f"spec.template.spec.containers[0].lifecycle.{HOOK_TYPE}", doc)
                == LIFECYCLE_PARSED
            )

    # Test Global container lifecycle hooks for the main services
    def test_global_setting_external(self):
        docs = render_chart(
            values={
                "containerLifecycleHooks": LIFECYCLE_CONFIG,
            },
            show_only=[
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
            ],
        )

        for doc in docs:
            assert jmespath.search("spec.template.spec.containers[0].lifecycle", doc) != LIFECYCLE_PARSED

    # <local>.containerLifecycleWebhooks > containerLifecycleWebhooks
    def test_check_main_container_setting(self):
        docs = render_chart(
            values={
                "containerLifecycleHooks": LIFECYCLE_CONFIG,
                "flower": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                "scheduler": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                "webserver": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                "migrateDatabaseJob": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                "triggerer": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                "redis": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                "statsd": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                "pgbouncer": {
                    "enabled": True,
                    "containerLifecycleHooks": LIFECYCLE_CONFIG,
                },
                "dagProcessor": {
                    "enabled": True,
                    "containerLifecycleHooks": LIFECYCLE_CONFIG,
                },
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert (
                jmespath.search(f"spec.template.spec.containers[0].lifecycle.{HOOK_TYPE}", doc)
                == LIFECYCLE_PARSED
            )

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"containerLifecycleHooks": LIFECYCLE_CONFIG},
            {"celery": {"containerLifecycleHooks": LIFECYCLE_CONFIG}},
            {
                "containerLifecycleHooks": {HOOK_TYPE: {"exec": {"command": ["echo", "preStop", "release"]}}},
                "celery": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
            },
        ],
    )
    def test_check_main_container_setting_workers(self, workers_values):
        docs = render_chart(
            values={
                "containerLifecycleHooks": LIFECYCLE_CONFIG,
                "workers": workers_values,
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
            ],
        )

        for doc in docs:
            assert (
                jmespath.search(f"spec.template.spec.containers[0].lifecycle.{HOOK_TYPE}", doc)
                == LIFECYCLE_PARSED
            )

    # Test container lifecycle hooks for metrics-explorer main container
    def test_metrics_explorer_container_setting(self):
        docs = render_chart(
            values={
                "pgbouncer": {
                    "enabled": True,
                    "metricsExporterSidecar": {"containerLifecycleHooks": LIFECYCLE_CONFIG},
                },
            },
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert (
            jmespath.search(f"spec.template.spec.containers[1].lifecycle.{HOOK_TYPE}", docs[0])
            == LIFECYCLE_PARSED
        )

    # Test container lifecycle hooks for worker-kerberos main container
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"kerberosSidecar": {"enabled": True, "containerLifecycleHooks": LIFECYCLE_CONFIG}},
            {"celery": {"kerberosSidecar": {"enabled": True, "containerLifecycleHooks": LIFECYCLE_CONFIG}}},
            {
                "kerberosSidecar": {
                    "enabled": True,
                    "containerLifecycleHooks": {
                        HOOK_TYPE: {"exec": {"command": ["echo", "preStop", "release"]}}
                    },
                },
                "celery": {"kerberosSidecar": {"enabled": True, "containerLifecycleHooks": LIFECYCLE_CONFIG}},
            },
        ],
    )
    def test_worker_kerberos_container_setting(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search(f"spec.template.spec.containers[2].lifecycle.{HOOK_TYPE}", docs[0])
            == LIFECYCLE_PARSED
        )

    # Test container lifecycle hooks for log-groomer-sidecar main container
    @pytest.mark.parametrize(
        "workers_values",
        [
            {"logGroomerSidecar": {"containerLifecycleHooks": LIFECYCLE_CONFIG}},
            {"celery": {"logGroomerSidecar": {"containerLifecycleHooks": LIFECYCLE_CONFIG}}},
            {
                "logGroomerSidecar": {
                    "containerLifecycleHooks": {
                        HOOK_TYPE: {"exec": {"command": ["echo", "preStop", "release"]}}
                    }
                },
                "celery": {"logGroomerSidecar": {"containerLifecycleHooks": LIFECYCLE_CONFIG}},
            },
        ],
    )
    def test_log_groomer_sidecar_container_setting(self, workers_values):
        docs = render_chart(
            values={
                "scheduler": {"logGroomerSidecar": {"containerLifecycleHooks": LIFECYCLE_CONFIG}},
                "workers": workers_values,
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        for doc in docs:
            assert (
                jmespath.search(f"spec.template.spec.containers[1].lifecycle.{HOOK_TYPE}", doc)
                == LIFECYCLE_PARSED
            )
