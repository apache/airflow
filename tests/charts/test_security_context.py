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

        for index in range(len(docs)):
            assert 3000 == jmespath.search("spec.template.spec.securityContext.runAsUser", docs[index])
            assert 30 == jmespath.search("spec.template.spec.securityContext.fsGroup", docs[index])

    def test_check_statsd_uid(self):
        docs = render_chart(
            values={"statsd": {"enabled": True, "uid": 3000}},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert 3000 == jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0])

    def test_check_cleanup_job(self):
        docs = render_chart(
            values={"uid": 3000, "gid": 30, "cleanup": {"enabled": True}},
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert 3000 == jmespath.search(
            "spec.jobTemplate.spec.template.spec.securityContext.runAsUser", docs[0]
        )
        assert 30 == jmespath.search("spec.jobTemplate.spec.template.spec.securityContext.fsGroup", docs[0])

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

        for index in range(len(docs)):
            assert "git-sync" in [
                c["name"] for c in jmespath.search("spec.template.spec.containers", docs[index])
            ]
            assert "git-sync-init" in [
                c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[index])
            ]
            assert 3000 == jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'].securityContext.runAsUser | [0]",
                docs[index],
            )
            assert 3000 == jmespath.search(
                "spec.template.spec.containers[?name=='git-sync'].securityContext.runAsUser | [0]",
                docs[index],
            )


class TestSecurityContext:
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

        for index in range(len(docs)):
            print(docs[index])
            assert 6000 == jmespath.search("spec.template.spec.securityContext.runAsUser", docs[index])
            assert 60 == jmespath.search("spec.template.spec.securityContext.fsGroup", docs[index])

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

        for index in range(len(docs)):
            print(docs[index])
            assert 9000 == jmespath.search("spec.template.spec.securityContext.runAsUser", docs[index])
            assert 90 == jmespath.search("spec.template.spec.securityContext.fsGroup", docs[index])

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
            assert 7000 == jmespath.search("spec.template.spec.securityContext.runAsUser", doc)

    # Test containerSecurity priority over uid under dags.gitSync
    def test_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={
                "dags": {"gitSync": {"enabled": True, "uid": 9000, "securityContext": {"runAsUser": 8000}}},
                "airflowVersion": "1.10.15",
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
            ],
        )

        for index in range(len(docs)):
            assert "git-sync" in [
                c["name"] for c in jmespath.search("spec.template.spec.containers", docs[index])
            ]
            assert "git-sync-init" in [
                c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[index])
            ]
            assert 8000 == jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'].securityContext.runAsUser | [0]",
                docs[index],
            )
            assert 8000 == jmespath.search(
                "spec.template.spec.containers[?name=='git-sync'].securityContext.runAsUser | [0]",
                docs[index],
            )
