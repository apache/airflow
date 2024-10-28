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

CONTAINER_LIFECYCLE_PARAMETERS = {
    "preStop": {
        "release_name": "test-release",
        "lifecycle_templated": {
            "exec": {"command": ["echo", "preStop", "{{ .Release.Name }}"]}
        },
        "lifecycle_parsed": {"exec": {"command": ["echo", "preStop", "test-release"]}},
    },
    "postStart": {
        "release_name": "test-release",
        "lifecycle_templated": {
            "exec": {"command": ["echo", "preStop", "{{ .Release.Name }}"]}
        },
        "lifecycle_parsed": {"exec": {"command": ["echo", "preStop", "test-release"]}},
    },
}


class TestContainerLifecycleHooks:
    """Tests container lifecycle hooks."""

    # Test container lifecycle hooks default setting
    def test_check_default_setting(self, hook_type="preStop"):
        lifecycle_hook_params = CONTAINER_LIFECYCLE_PARAMETERS[hook_type]
        docs = render_chart(
            name=lifecycle_hook_params["release_name"],
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
            assert (
                jmespath.search("spec.template.spec.containers[0].lifecycle", doc) is None
            )

        pgbouncer_default_value = {
            "exec": {"command": ["/bin/sh", "-c", "killall -INT pgbouncer && sleep 120"]}
        }
        assert pgbouncer_default_value == jmespath.search(
            "spec.template.spec.containers[0].lifecycle.preStop", docs[-1]
        )
        assert (
            jmespath.search(
                "spec.template.spec.containers[0].lifecycle.postStart", docs[-1]
            )
            is None
        )

    # Test Global container lifecycle hooks for the main services
    def test_global_setting(self, hook_type="preStop"):
        lifecycle_hook_params = CONTAINER_LIFECYCLE_PARAMETERS[hook_type]
        docs = render_chart(
            name=lifecycle_hook_params["release_name"],
            values={
                "containerLifecycleHooks": {
                    hook_type: lifecycle_hook_params["lifecycle_templated"]
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
            ],
        )

        for doc in docs:
            assert lifecycle_hook_params["lifecycle_parsed"] == jmespath.search(
                f"spec.template.spec.containers[0].lifecycle.{hook_type}", doc
            )

    # Test Global container lifecycle hooks for the main services
    def test_global_setting_external(self, hook_type="preStop"):
        lifecycle_hook_params = CONTAINER_LIFECYCLE_PARAMETERS[hook_type]
        lifecycle_hooks_config = {hook_type: lifecycle_hook_params["lifecycle_templated"]}
        docs = render_chart(
            name=lifecycle_hook_params["release_name"],
            values={
                "containerLifecycleHooks": lifecycle_hooks_config,
            },
            show_only=[
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
            ],
        )

        for doc in docs:
            assert lifecycle_hook_params["lifecycle_parsed"] != jmespath.search(
                "spec.template.spec.containers[0].lifecycle", doc
            )

    # <local>.containerLifecycleWebhooks > containerLifecycleWebhooks
    def test_check_main_container_setting(self, hook_type="preStop"):
        lifecycle_hook_params = CONTAINER_LIFECYCLE_PARAMETERS[hook_type]
        lifecycle_hooks_config = {hook_type: lifecycle_hook_params["lifecycle_templated"]}
        docs = render_chart(
            name=lifecycle_hook_params["release_name"],
            values={
                "containerLifecycleHooks": lifecycle_hooks_config,
                "flower": {"containerLifecycleHooks": lifecycle_hooks_config},
                "scheduler": {"containerLifecycleHooks": lifecycle_hooks_config},
                "webserver": {"containerLifecycleHooks": lifecycle_hooks_config},
                "workers": {"containerLifecycleHooks": lifecycle_hooks_config},
                "migrateDatabaseJob": {"containerLifecycleHooks": lifecycle_hooks_config},
                "triggerer": {"containerLifecycleHooks": lifecycle_hooks_config},
                "redis": {"containerLifecycleHooks": lifecycle_hooks_config},
                "statsd": {"containerLifecycleHooks": lifecycle_hooks_config},
                "pgbouncer": {
                    "enabled": True,
                    "containerLifecycleHooks": lifecycle_hooks_config,
                },
                "dagProcessor": {
                    "enabled": True,
                    "containerLifecycleHooks": lifecycle_hooks_config,
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
            assert lifecycle_hook_params["lifecycle_parsed"] == jmespath.search(
                f"spec.template.spec.containers[0].lifecycle.{hook_type}", doc
            )

    # Test container lifecycle hooks for metrics-explorer main container
    def test_metrics_explorer_container_setting(self, hook_type="preStop"):
        lifecycle_hook_params = CONTAINER_LIFECYCLE_PARAMETERS[hook_type]
        lifecycle_hooks_config = {hook_type: lifecycle_hook_params["lifecycle_templated"]}
        docs = render_chart(
            name=lifecycle_hook_params["release_name"],
            values={
                "pgbouncer": {
                    "enabled": True,
                    "metricsExporterSidecar": {
                        "containerLifecycleHooks": lifecycle_hooks_config
                    },
                },
            },
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert lifecycle_hook_params["lifecycle_parsed"] == jmespath.search(
            f"spec.template.spec.containers[1].lifecycle.{hook_type}", docs[0]
        )

    # Test container lifecycle hooks for worker-kerberos main container
    def test_worker_kerberos_container_setting(self, hook_type="preStop"):
        lifecycle_hook_params = CONTAINER_LIFECYCLE_PARAMETERS[hook_type]
        lifecycle_hooks_config = {hook_type: lifecycle_hook_params["lifecycle_templated"]}
        docs = render_chart(
            name=lifecycle_hook_params["release_name"],
            values={
                "workers": {
                    "kerberosSidecar": {
                        "enabled": True,
                        "containerLifecycleHooks": lifecycle_hooks_config,
                    }
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert lifecycle_hook_params["lifecycle_parsed"] == jmespath.search(
            f"spec.template.spec.containers[2].lifecycle.{hook_type}", docs[0]
        )

    # Test container lifecycle hooks for log-groomer-sidecar main container
    def test_log_groomer_sidecar_container_setting(self, hook_type="preStop"):
        lifecycle_hook_params = CONTAINER_LIFECYCLE_PARAMETERS[hook_type]
        lifecycle_hooks_config = {hook_type: lifecycle_hook_params["lifecycle_templated"]}
        docs = render_chart(
            name=lifecycle_hook_params["release_name"],
            values={
                "scheduler": {
                    "logGroomerSidecar": {
                        "containerLifecycleHooks": lifecycle_hooks_config
                    }
                },
                "workers": {
                    "logGroomerSidecar": {
                        "containerLifecycleHooks": lifecycle_hooks_config
                    }
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        for doc in docs:
            assert lifecycle_hook_params["lifecycle_parsed"] == jmespath.search(
                f"spec.template.spec.containers[1].lifecycle.{hook_type}", doc
            )
