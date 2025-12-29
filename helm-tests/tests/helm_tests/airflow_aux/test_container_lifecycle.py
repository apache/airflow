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

RELEASE_NAME = "test-release"
LIFECYCLE_TEMPLATE = {"exec": {"command": ["echo", "{{ .Release.Name }}"]}}
LIFECYCLE_PARSED = {"exec": {"command": ["echo", RELEASE_NAME]}}


class TestContainerLifecycleHooks:
    """Tests container lifecycle hooks."""

    # Test container lifecycle hooks default setting
    def test_check_default_setting(self):
        docs = render_chart(
            name=RELEASE_NAME,
            values={
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
    @pytest.mark.parametrize("hook_type", ["preStop", "postStart"])
    def test_global_setting(self, hook_type):
        docs = render_chart(
            name=RELEASE_NAME,
            values={
                "containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE},
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
                jmespath.search(f"spec.template.spec.containers[0].lifecycle.{hook_type}", doc)
                == LIFECYCLE_PARSED
            )

    # Test Global container lifecycle hooks for the main services
    @pytest.mark.parametrize("hook_type", ["preStop", "postStart"])
    def test_global_setting_external(self, hook_type):
        docs = render_chart(
            name=RELEASE_NAME,
            values={
                "containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE},
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
    @pytest.mark.parametrize("hook_type", ["preStop", "postStart"])
    def test_check_main_container_setting(self, hook_type):
        docs = render_chart(
            name=RELEASE_NAME,
            values={
                "containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE},
                "flower": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "scheduler": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "webserver": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "workers": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "migrateDatabaseJob": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "triggerer": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "redis": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "statsd": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                "pgbouncer": {
                    "enabled": True,
                    "containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE},
                },
                "dagProcessor": {
                    "enabled": True,
                    "containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE},
                },
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
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert (
                jmespath.search(f"spec.template.spec.containers[0].lifecycle.{hook_type}", doc)
                == LIFECYCLE_PARSED
            )

    # Test container lifecycle hooks for metrics-explorer main container
    @pytest.mark.parametrize("hook_type", ["preStop", "postStart"])
    def test_metrics_explorer_container_setting(self, hook_type):
        docs = render_chart(
            name=RELEASE_NAME,
            values={
                "pgbouncer": {
                    "enabled": True,
                    "metricsExporterSidecar": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}},
                },
            },
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert (
            jmespath.search(f"spec.template.spec.containers[1].lifecycle.{hook_type}", docs[0])
            == LIFECYCLE_PARSED
        )

    # Test container lifecycle hooks for worker-kerberos main container
    @pytest.mark.parametrize("hook_type", ["preStop", "postStart"])
    def test_worker_kerberos_container_setting(self, hook_type):
        docs = render_chart(
            name=RELEASE_NAME,
            values={
                "workers": {
                    "kerberosSidecar": {
                        "enabled": True,
                        "containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE},
                    }
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search(f"spec.template.spec.containers[2].lifecycle.{hook_type}", docs[0])
            == LIFECYCLE_PARSED
        )

    # Test container lifecycle hooks for log-groomer-sidecar main container
    @pytest.mark.parametrize("hook_type", ["preStop", "postStart"])
    def test_log_groomer_sidecar_container_setting(self, hook_type):
        docs = render_chart(
            name=RELEASE_NAME,
            values={
                "scheduler": {
                    "logGroomerSidecar": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}}
                },
                "workers": {
                    "logGroomerSidecar": {"containerLifecycleHooks": {hook_type: LIFECYCLE_TEMPLATE}}
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        for doc in docs:
            assert (
                jmespath.search(f"spec.template.spec.containers[1].lifecycle.{hook_type}", doc)
                == LIFECYCLE_PARSED
            )
