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

import copy

import jmespath
import pytest
import yaml
from chart_utils.helm_template_generator import render_chart


def deployment_annotations(obj):
    return obj["spec"]["template"]["metadata"]["annotations"]


def cronjob_annotations(obj):
    return obj["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["annotations"]


def get_object_annotations(obj):
    if obj["kind"] == "CronJob":
        return cronjob_annotations(obj)
    return deployment_annotations(obj)


class TestServiceAccountAnnotations:
    """Tests Service Account Annotations."""

    @pytest.mark.parametrize(
        ("values", "show_only", "expected_annotations"),
        [
            (
                {
                    "executor": "KubernetesExecutor",
                    "cleanup": {
                        "enabled": True,
                        "serviceAccount": {
                            "annotations": {
                                "example": "cleanup",
                            },
                        },
                    },
                },
                "templates/cleanup/cleanup-serviceaccount.yaml",
                {
                    "example": "cleanup",
                },
            ),
            (
                {
                    "databaseCleanup": {
                        "enabled": True,
                        "serviceAccount": {
                            "annotations": {
                                "example": "database-cleanup",
                            },
                        },
                    },
                },
                "templates/database-cleanup/database-cleanup-serviceaccount.yaml",
                {
                    "example": "database-cleanup",
                },
            ),
            (
                {
                    "scheduler": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "scheduler",
                            },
                        },
                    },
                },
                "templates/scheduler/scheduler-serviceaccount.yaml",
                {
                    "example": "scheduler",
                },
            ),
            (
                {
                    "apiServer": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "api-server",
                            },
                        },
                    },
                },
                "templates/api-server/api-server-serviceaccount.yaml",
                {
                    "example": "api-server",
                },
            ),
            (
                {
                    "workers": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "worker",
                            },
                        },
                    },
                },
                "templates/workers/worker-serviceaccount.yaml",
                {
                    "example": "worker",
                },
            ),
            (
                {
                    "workers": {
                        "celery": {
                            "serviceAccount": {
                                "annotations": {
                                    "example": "worker",
                                },
                            }
                        },
                    },
                },
                "templates/workers/worker-serviceaccount.yaml",
                {
                    "example": "worker",
                },
            ),
            (
                {
                    "workers": {
                        "serviceAccount": {
                            "annotations": {
                                "worker": "example",
                            },
                        },
                        "celery": {
                            "serviceAccount": {
                                "annotations": {
                                    "example": "worker",
                                },
                            }
                        },
                    },
                },
                "templates/workers/worker-serviceaccount.yaml",
                {
                    "example": "worker",
                },
            ),
            (
                {
                    "executor": "KubernetesExecutor",
                    "workers": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "worker",
                            },
                        },
                        "kubernetes": {"serviceAccount": {"create": True}},
                    },
                },
                "templates/workers/worker-kubernetes-serviceaccount.yaml",
                {
                    "example": "worker",
                },
            ),
            (
                {
                    "executor": "KubernetesExecutor",
                    "workers": {
                        "kubernetes": {
                            "serviceAccount": {
                                "create": True,
                                "annotations": {
                                    "example": "worker",
                                },
                            }
                        },
                    },
                },
                "templates/workers/worker-kubernetes-serviceaccount.yaml",
                {
                    "example": "worker",
                },
            ),
            (
                {
                    "executor": "KubernetesExecutor",
                    "workers": {
                        "serviceAccount": {
                            "annotations": {
                                "worker": "example",
                            },
                        },
                        "kubernetes": {
                            "serviceAccount": {
                                "create": True,
                                "annotations": {
                                    "example": "worker",
                                },
                            }
                        },
                    },
                },
                "templates/workers/worker-kubernetes-serviceaccount.yaml",
                {
                    "example": "worker",
                },
            ),
            (
                {
                    "flower": {
                        "enabled": True,
                        "serviceAccount": {
                            "annotations": {
                                "example": "flower",
                            },
                        },
                    },
                },
                "templates/flower/flower-serviceaccount.yaml",
                {
                    "example": "flower",
                },
            ),
            (
                {
                    "statsd": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "statsd",
                            },
                        },
                    },
                },
                "templates/statsd/statsd-serviceaccount.yaml",
                {
                    "example": "statsd",
                },
            ),
            (
                {
                    "redis": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "redis",
                            },
                        },
                    },
                },
                "templates/redis/redis-serviceaccount.yaml",
                {
                    "example": "redis",
                },
            ),
            (
                {
                    "pgbouncer": {
                        "enabled": True,
                        "serviceAccount": {
                            "annotations": {
                                "example": "pgbouncer",
                            },
                        },
                    },
                },
                "templates/pgbouncer/pgbouncer-serviceaccount.yaml",
                {
                    "example": "pgbouncer",
                },
            ),
            (
                {
                    "createUserJob": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "createuser",
                            },
                        },
                    },
                },
                "templates/jobs/create-user-job-serviceaccount.yaml",
                {
                    "example": "createuser",
                },
            ),
            (
                {
                    "migrateDatabaseJob": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "migratedb",
                            },
                        },
                    },
                },
                "templates/jobs/migrate-database-job-serviceaccount.yaml",
                {
                    "example": "migratedb",
                },
            ),
            (
                {
                    "triggerer": {
                        "serviceAccount": {
                            "annotations": {
                                "example": "triggerer",
                            },
                        },
                    },
                },
                "templates/triggerer/triggerer-serviceaccount.yaml",
                {
                    "example": "triggerer",
                },
            ),
            (
                {
                    "dagProcessor": {
                        "enabled": True,
                        "serviceAccount": {
                            "annotations": {
                                "example": "dag-processor",
                            },
                        },
                    },
                },
                "templates/dag-processor/dag-processor-serviceaccount.yaml",
                {
                    "example": "dag-processor",
                },
            ),
        ],
    )
    def test_annotations_are_added(self, values, show_only, expected_annotations):
        k8s_objects = render_chart(
            values=values,
            show_only=[show_only],
        )

        # This test relies on the convention that the helm chart puts a single
        # ServiceAccount in its own .yaml file, so by specifying `show_only`,
        # we should only get a single k8s_object here - the target object that
        # we hope to test on.
        assert len(k8s_objects) == 1
        obj = k8s_objects[0]

        for k, v in expected_annotations.items():
            assert k in obj["metadata"]["annotations"]
            assert v == obj["metadata"]["annotations"][k]

    def test_annotations_on_webserver(self):
        """Test annotations are added on webserver for Airflow 2"""
        k8s_objects = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "webserver": {
                    "serviceAccount": {
                        "annotations": {
                            "example": "webserver",
                        },
                    },
                },
            },
            show_only=["templates/webserver/webserver-serviceaccount.yaml"],
        )

        assert len(k8s_objects) == 1
        obj = k8s_objects[0]

        assert obj["metadata"]["annotations"] == {"example": "webserver"}


@pytest.mark.parametrize(
    ("values", "show_only", "expected_annotations"),
    [
        (
            {
                "scheduler": {
                    "podAnnotations": {
                        "example": "scheduler",
                    },
                },
            },
            "templates/scheduler/scheduler-deployment.yaml",
            {
                "example": "scheduler",
            },
        ),
        (
            {
                "apiServer": {
                    "podAnnotations": {
                        "example": "api-server",
                    },
                },
            },
            "templates/api-server/api-server-deployment.yaml",
            {
                "example": "api-server",
            },
        ),
        (
            {
                "workers": {
                    "podAnnotations": {
                        "example": "worker",
                    },
                },
            },
            "templates/workers/worker-deployment.yaml",
            {
                "example": "worker",
            },
        ),
        (
            {
                "flower": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "flower",
                    },
                },
            },
            "templates/flower/flower-deployment.yaml",
            {
                "example": "flower",
            },
        ),
        (
            {
                "triggerer": {
                    "podAnnotations": {
                        "example": "triggerer",
                    },
                },
            },
            "templates/triggerer/triggerer-deployment.yaml",
            {
                "example": "triggerer",
            },
        ),
        (
            {
                "dagProcessor": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "dag-processor",
                    },
                },
            },
            "templates/dag-processor/dag-processor-deployment.yaml",
            {
                "example": "dag-processor",
            },
        ),
        (
            {
                "executor": "KubernetesExecutor",
                "cleanup": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "cleanup",
                    },
                },
            },
            "templates/cleanup/cleanup-cronjob.yaml",
            {
                "example": "cleanup",
            },
        ),
        (
            {
                "databaseCleanup": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "database-cleanup",
                    },
                }
            },
            "templates/database-cleanup/database-cleanup-cronjob.yaml",
            {
                "example": "database-cleanup",
            },
        ),
        (
            {
                "redis": {
                    "podAnnotations": {
                        "example": "redis",
                    },
                },
            },
            "templates/redis/redis-statefulset.yaml",
            {
                "example": "redis",
            },
        ),
        (
            {
                "statsd": {
                    "podAnnotations": {
                        "example": "statsd",
                    },
                },
            },
            "templates/statsd/statsd-deployment.yaml",
            {
                "example": "statsd",
            },
        ),
        (
            {
                "pgbouncer": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "pgbouncer",
                    },
                },
            },
            "templates/pgbouncer/pgbouncer-deployment.yaml",
            {
                "example": "pgbouncer",
            },
        ),
    ],
)
class TestPerComponentPodAnnotations:
    """Tests Per Component Pod Annotations."""

    def test_annotations_are_added(self, values, show_only, expected_annotations):
        k8s_objects = render_chart(
            values=values,
            show_only=[show_only],
        )

        # This test relies on the convention that the helm chart puts a single
        # Deployment in its own .yaml file, so by specifying `show_only`,
        # we should only get a single k8s_object here - the target object that
        # we hope to test on.
        assert len(k8s_objects) == 1
        obj = k8s_objects[0]
        annotations = get_object_annotations(obj)

        for k, v in expected_annotations.items():
            assert k in annotations
            assert v == annotations[k]

    def test_precedence(self, values, show_only, expected_annotations):
        values_global_annotations = {"airflowPodAnnotations": {k: "GLOBAL" for k in expected_annotations}}

        values_merged = {**values, **values_global_annotations}

        k8s_objects = render_chart(
            values=values_merged,
            show_only=[show_only],
        )

        # This test relies on the convention that the helm chart puts a single
        # Deployment in its own .yaml file, so by specifying `show_only`,
        # we should only get a single k8s_object here - the target object that
        # we hope to test on.
        assert len(k8s_objects) == 1
        obj = k8s_objects[0]
        annotations = get_object_annotations(obj)

        for k, v in expected_annotations.items():
            assert k in annotations
            assert v == annotations[k]

    def test_pod_annotations_are_templated(self, values, show_only, expected_annotations):
        templated_values = copy.deepcopy(values)
        for val in templated_values.values():
            if isinstance(val, dict) and "podAnnotations" in val:
                val["podAnnotations"] = {"release-name": "{{ .Release.Name }}"}

        k8s_objects = render_chart(
            values=templated_values,
            show_only=[show_only],
        )

        assert len(k8s_objects) == 1
        annotations = get_object_annotations(k8s_objects[0])
        assert annotations["release-name"] == "release-name"

    def test_airflow_pod_annotations_are_templated(self, values, show_only, expected_annotations):
        templated_values = copy.deepcopy(values)
        templated_values["airflowPodAnnotations"] = {"global-release": "{{ .Release.Name }}"}

        k8s_objects = render_chart(
            values=templated_values,
            show_only=[show_only],
        )

        assert len(k8s_objects) == 1
        annotations = get_object_annotations(k8s_objects[0])
        # pgbouncer, statsd, and redis do not render airflowPodAnnotations
        if "global-release" in annotations:
            assert annotations["global-release"] == "release-name"
        else:
            assert show_only in (
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
            )


class TestRedisAnnotations:
    """Tests Redis Annotations."""

    def test_redis_annotations_are_added(self):
        # Test Case
        values = {"redis": {"annotations": {"example": "redis"}}}
        show_only = "templates/redis/redis-statefulset.yaml"
        expected_annotations = {"example": "redis"}

        k8s_objects = render_chart(
            values=values,
            show_only=[show_only],
        )

        # This test relies on the convention that the helm chart puts annotations
        # in its own .yaml file, so by specifying `show_only`,
        # we should only get a single k8s_object here - the target object that
        # we hope to test on.
        assert len(k8s_objects) == 1
        obj = k8s_objects[0]

        for k, v in expected_annotations.items():
            assert k in obj["metadata"]["annotations"]
            assert v == obj["metadata"]["annotations"][k]


class TestPodTemplateFileAnnotationsTemplating:
    """Tests that podAnnotations are templated in the pod template file."""

    def test_pod_template_file_annotations_are_templated(self):
        k8s_objects = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {
                    "podAnnotations": {
                        "release-name": "{{ .Release.Name }}",
                    },
                },
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        assert len(k8s_objects) == 1
        pod_template = k8s_objects[0]["data"]["pod_template_file.yaml"]
        annotations = jmespath.search(
            "metadata.annotations",
            yaml.safe_load(pod_template),
        )
        assert annotations["release-name"] == "release-name"

    def test_pod_template_file_global_annotations_are_templated(self):
        k8s_objects = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "airflowPodAnnotations": {
                    "global-release": "{{ .Release.Name }}",
                },
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        assert len(k8s_objects) == 1
        pod_template = k8s_objects[0]["data"]["pod_template_file.yaml"]
        annotations = jmespath.search(
            "metadata.annotations",
            yaml.safe_load(pod_template),
        )
        assert annotations["global-release"] == "release-name"


class TestWebserverPodAnnotationsTemplating:
    """Tests webserver podAnnotations templating (requires airflowVersion < 3.0.0)."""

    def test_webserver_pod_annotations_are_templated(self):
        k8s_objects = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "webserver": {
                    "podAnnotations": {
                        "release-name": "{{ .Release.Name }}",
                    },
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert len(k8s_objects) == 1
        annotations = get_object_annotations(k8s_objects[0])
        assert annotations["release-name"] == "release-name"

    def test_webserver_airflow_pod_annotations_are_templated(self):
        k8s_objects = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "airflowPodAnnotations": {
                    "global-release": "{{ .Release.Name }}",
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert len(k8s_objects) == 1
        annotations = get_object_annotations(k8s_objects[0])
        assert annotations["global-release"] == "release-name"


class TestJobAnnotationsTemplating:
    """Tests that annotations are templated in job templates."""

    @pytest.mark.parametrize(
        ("values", "show_only"),
        [
            (
                {"createUserJob": {"annotations": {"job-ann": "{{ .Release.Name }}"}}},
                "templates/jobs/create-user-job.yaml",
            ),
            (
                {"migrateDatabaseJob": {"annotations": {"job-ann": "{{ .Release.Name }}"}}},
                "templates/jobs/migrate-database-job.yaml",
            ),
        ],
    )
    def test_job_annotations_are_templated(self, values, show_only):
        templated_values = copy.deepcopy(values)
        templated_values["airflowPodAnnotations"] = {"global-ann": "{{ .Release.Name }}"}
        k8s_objects = render_chart(
            values=templated_values,
            show_only=[show_only],
        )

        assert len(k8s_objects) == 1
        annotations = k8s_objects[0]["spec"]["template"]["metadata"]["annotations"]
        assert annotations["global-ann"] == "release-name"
        assert annotations["job-ann"] == "release-name"
