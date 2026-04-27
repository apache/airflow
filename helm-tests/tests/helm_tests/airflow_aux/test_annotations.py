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

import pytest
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

    def test_tpl_rendered_multiple_annotations(self):
        """Test that multiple annotations render correctly with tpl."""
        k8s_objects = render_chart(
            values={
                "scheduler": {
                    "serviceAccount": {
                        "annotations": {
                            "iam.gke.io/gcp-service-account": "{{ .Release.Name }}-sa@project.iam",
                            "another-annotation": "{{ .Release.Name }}-other",
                            "plain-annotation": "no-template",
                        },
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-serviceaccount.yaml"],
        )
        assert len(k8s_objects) == 1
        annotations = k8s_objects[0]["metadata"]["annotations"]
        assert annotations["iam.gke.io/gcp-service-account"] == "release-name-sa@project.iam"
        assert annotations["another-annotation"] == "release-name-other"
        assert annotations["plain-annotation"] == "no-template"

    def test_tpl_rendered_annotations_pgbouncer(self):
        """Test pgbouncer SA annotations support tpl rendering."""
        k8s_objects = render_chart(
            values={
                "pgbouncer": {
                    "enabled": True,
                    "serviceAccount": {
                        "annotations": {
                            "iam.gke.io/gcp-service-account": "{{ .Release.Name }}-sa@project.iam",
                        },
                    },
                },
            },
            show_only=["templates/pgbouncer/pgbouncer-serviceaccount.yaml"],
        )
        assert len(k8s_objects) == 1
        annotations = k8s_objects[0]["metadata"]["annotations"]
        assert annotations["iam.gke.io/gcp-service-account"] == "release-name-sa@project.iam"

    @pytest.mark.parametrize(
        ("values_key", "show_only"),
        [
            ("dagProcessor", "templates/dag-processor/dag-processor-serviceaccount.yaml"),
            ("apiServer", "templates/api-server/api-server-serviceaccount.yaml"),
        ],
    )
    def test_tpl_rendered_annotations_airflow_3(self, values_key, show_only):
        """Test SA annotations support tpl rendering for Airflow 3.x components."""
        k8s_objects = render_chart(
            values={
                values_key: {
                    "serviceAccount": {
                        "annotations": {
                            "iam.gke.io/gcp-service-account": "{{ .Release.Name }}-sa@project.iam",
                        },
                    },
                },
            },
            show_only=[show_only],
        )
        assert len(k8s_objects) == 1
        annotations = k8s_objects[0]["metadata"]["annotations"]
        assert annotations["iam.gke.io/gcp-service-account"] == "release-name-sa@project.iam"

    def test_tpl_rendered_annotations_celery_worker(self):
        """Test Celery worker SA annotations support tpl rendering."""
        k8s_objects = render_chart(
            values={
                "workers": {
                    "celery": {
                        "serviceAccount": {
                            "annotations": {
                                "iam.gke.io/gcp-service-account": "{{ .Release.Name }}-worker@project.iam",
                            },
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )
        assert len(k8s_objects) == 1
        annotations = k8s_objects[0]["metadata"]["annotations"]
        assert annotations["iam.gke.io/gcp-service-account"] == "release-name-worker@project.iam"

    def test_tpl_rendered_annotations_kubernetes_worker(self):
        """Test KubernetesExecutor worker SA annotations support tpl rendering."""
        k8s_objects = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {
                    "serviceAccount": {
                        "annotations": {
                            "iam.gke.io/gcp-service-account": "{{ .Release.Name }}-worker@project.iam",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )
        assert len(k8s_objects) == 1
        annotations = k8s_objects[0]["metadata"]["annotations"]
        assert annotations["iam.gke.io/gcp-service-account"] == "release-name-worker@project.iam"


@pytest.mark.parametrize(
    ("values", "show_only", "expected_annotations"),
    [
        (
            {
                "scheduler": {
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-scheduler",
                    },
                },
            },
            "templates/scheduler/scheduler-deployment.yaml",
            {
                "example": "release-name-scheduler",
            },
        ),
        (
            {
                "apiServer": {
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-api-server",
                    },
                },
            },
            "templates/api-server/api-server-deployment.yaml",
            {
                "example": "release-name-api-server",
            },
        ),
        (
            {
                "workers": {
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-worker",
                    },
                },
            },
            "templates/workers/worker-deployment.yaml",
            {
                "example": "release-name-worker",
            },
        ),
        (
            {
                "workers": {
                    "celery": {
                        "podAnnotations": {
                            "example": "{{ .Release.Name }}-worker",
                        },
                    }
                },
            },
            "templates/workers/worker-deployment.yaml",
            {
                "example": "release-name-worker",
            },
        ),
        (
            {
                "workers": {
                    "podAnnotations": {
                        "test": "test",
                    },
                    "celery": {
                        "podAnnotations": {
                            "example": "{{ .Release.Name }}-worker",
                        },
                    },
                },
            },
            "templates/workers/worker-deployment.yaml",
            {
                "example": "release-name-worker",
            },
        ),
        (
            {
                "flower": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-flower",
                    },
                },
            },
            "templates/flower/flower-deployment.yaml",
            {
                "example": "release-name-flower",
            },
        ),
        (
            {
                "triggerer": {
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-triggerer",
                    },
                },
            },
            "templates/triggerer/triggerer-deployment.yaml",
            {
                "example": "release-name-triggerer",
            },
        ),
        (
            {
                "dagProcessor": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-dag-processor",
                    },
                },
            },
            "templates/dag-processor/dag-processor-deployment.yaml",
            {
                "example": "release-name-dag-processor",
            },
        ),
        (
            {
                "executor": "KubernetesExecutor",
                "cleanup": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-cleanup",
                    },
                },
            },
            "templates/cleanup/cleanup-cronjob.yaml",
            {
                "example": "release-name-cleanup",
            },
        ),
        (
            {
                "databaseCleanup": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-database-cleanup",
                    },
                }
            },
            "templates/database-cleanup/database-cleanup-cronjob.yaml",
            {
                "example": "release-name-database-cleanup",
            },
        ),
        (
            {
                "redis": {
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-redis",
                    },
                },
            },
            "templates/redis/redis-statefulset.yaml",
            {
                "example": "release-name-redis",
            },
        ),
        (
            {
                "statsd": {
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-statsd",
                    },
                },
            },
            "templates/statsd/statsd-deployment.yaml",
            {
                "example": "release-name-statsd",
            },
        ),
        (
            {
                "pgbouncer": {
                    "enabled": True,
                    "podAnnotations": {
                        "example": "{{ .Release.Name }}-pgbouncer",
                    },
                },
            },
            "templates/pgbouncer/pgbouncer-deployment.yaml",
            {
                "example": "release-name-pgbouncer",
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
        k8s_objects = render_chart(
            values=values,
            show_only=[show_only],
        )

        assert len(k8s_objects) == 1
        annotations = get_object_annotations(k8s_objects[0])
        assert annotations["example"] == expected_annotations["example"]
        assert "test" not in annotations

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
