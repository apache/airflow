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
import yaml
from chart_utils.helm_template_generator import CHART_DIR, render_chart


def _get_default_airflow_image() -> tuple[str, str]:
    """Read the default airflow image and tag from chart/values.yaml."""
    values = yaml.safe_load((CHART_DIR / "values.yaml").read_text())
    repo = values["defaultAirflowRepository"]
    tag = values["defaultAirflowTag"]
    return f"{repo}:{tag}", tag


DEFAULT_AIRFLOW_IMAGE, DEFAULT_AIRFLOW_TAG = _get_default_airflow_image()


def _all_airflow_images(doc):
    """Extract all airflow images from a rendered worker deployment/statefulset."""
    init_images = jmespath.search("spec.template.spec.initContainers[*].image", doc) or []
    container_images = jmespath.search("spec.template.spec.containers[*].image", doc) or []
    # Filter out git-sync sidecar images (they don't use airflow_worker_image)
    all_images = init_images + container_images
    return [img for img in all_images if "git-sync" not in img]


def _all_airflow_pull_policies(doc):
    """Extract all imagePullPolicy values from airflow containers."""
    init_policies = jmespath.search("spec.template.spec.initContainers[*].imagePullPolicy", doc) or []
    container_policies = jmespath.search("spec.template.spec.containers[*].imagePullPolicy", doc) or []
    return init_policies + container_policies


class TestWorkerImageDefault:
    """Tests that workers use the default airflow image when no override is set."""

    def test_default_image_used_when_no_override(self):
        """When no worker image override is set, all worker containers use the global default image."""
        docs = render_chart(
            values={"executor": "CeleryExecutor"},
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert len(docs) == 1
        images = _all_airflow_images(docs[0])
        assert len(images) > 0
        for image in images:
            assert image == DEFAULT_AIRFLOW_IMAGE

    def test_default_image_with_global_override(self):
        """When images.airflow is set, workers use it (no worker-specific override)."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "images": {
                    "airflow": {
                        "repository": "my-registry/my-airflow",
                        "tag": "custom-v1",
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == "my-registry/my-airflow:custom-v1"


class TestWorkerImageCeleryOverride:
    """Tests that workers.celery.image overrides the global airflow image."""

    def test_celery_image_overrides_default(self):
        """Setting workers.celery.image.repository/tag overrides the default for all worker containers."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "celery-custom/airflow",
                            "tag": "celery-v1",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        assert len(images) > 0
        for image in images:
            assert image == "celery-custom/airflow:celery-v1"

    def test_celery_image_overrides_global_image(self):
        """workers.celery.image takes precedence over images.airflow."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "images": {
                    "airflow": {
                        "repository": "global/airflow",
                        "tag": "global-v1",
                    },
                },
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "celery-custom/airflow",
                            "tag": "celery-v2",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == "celery-custom/airflow:celery-v2"

    def test_celery_image_partial_override_repository_only(self):
        """Setting only repository at celery level falls back to default tag."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "celery-custom/airflow",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == f"celery-custom/airflow:{DEFAULT_AIRFLOW_TAG}"

    def test_celery_image_partial_override_tag_only(self):
        """Setting only tag at celery level falls back to default repository."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "tag": "custom-tag",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == "apache/airflow:custom-tag"

    def test_celery_image_digest_takes_precedence_over_tag(self):
        """When digest is set, it takes precedence over tag."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "celery-custom/airflow",
                            "tag": "should-be-ignored",
                            "digest": "sha256:abcdef1234567890",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == "celery-custom/airflow@sha256:abcdef1234567890"


class TestWorkerImagePerSetOverride:
    """Tests that per-set image overrides work correctly."""

    def test_per_set_image_overrides_celery_image(self):
        """A worker set with its own image overrides the celery-level image."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "image": {
                            "repository": "celery-base/airflow",
                            "tag": "base-v1",
                        },
                        "sets": [
                            {
                                "name": "gpu",
                                "queue": "gpu",
                                "image": {
                                    "repository": "my-gpu/airflow",
                                    "tag": "cuda-v1",
                                },
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert len(docs) == 1
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == "my-gpu/airflow:cuda-v1"

    def test_default_set_uses_celery_image_others_override(self):
        """Default set uses celery-level image while named sets use their own."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": True,
                        "image": {
                            "repository": "celery-base/airflow",
                            "tag": "base-v1",
                        },
                        "sets": [
                            {
                                "name": "gpu",
                                "queue": "gpu",
                                "image": {
                                    "repository": "my-gpu/airflow",
                                    "tag": "cuda-v1",
                                },
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert len(docs) == 2

        # First doc is the default worker
        default_images = _all_airflow_images(docs[0])
        for image in default_images:
            assert image == "celery-base/airflow:base-v1"

        # Second doc is the gpu worker set
        gpu_images = _all_airflow_images(docs[1])
        for image in gpu_images:
            assert image == "my-gpu/airflow:cuda-v1"

    def test_per_set_partial_override_falls_back_to_global_default_tag(self):
        """Per-set image with only repository falls back to global default tag, not celery-level tag."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "image": {
                            "repository": "celery-base/airflow",
                            "tag": "base-v1",
                        },
                        "sets": [
                            {
                                "name": "gpu",
                                "queue": "gpu",
                                "image": {
                                    "repository": "my-gpu/airflow",
                                },
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        # "image" is in fullOverwrite list, so per-set image fully replaces celery image.
        # With only repository set, tag falls back to global default, not celery-level tag.
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == f"my-gpu/airflow:{DEFAULT_AIRFLOW_TAG}"

    def test_per_set_digest_override(self):
        """Per-set image with digest takes precedence over tag."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [
                            {
                                "name": "special",
                                "queue": "special",
                                "image": {
                                    "repository": "special/airflow",
                                    "digest": "sha256:deadbeef",
                                },
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == "special/airflow@sha256:deadbeef"

    def test_set_without_image_uses_celery_image(self):
        """Worker set without image override uses the celery-level image."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "image": {
                            "repository": "celery-base/airflow",
                            "tag": "base-v1",
                        },
                        "sets": [
                            {
                                "name": "no-image-override",
                                "queue": "default",
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == "celery-base/airflow:base-v1"

    def test_set_without_image_no_celery_image_uses_default(self):
        """Worker set without image override and no celery-level image uses global default."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [
                            {
                                "name": "plain",
                                "queue": "plain",
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        for image in images:
            assert image == DEFAULT_AIRFLOW_IMAGE

    def test_multiple_sets_with_different_images(self):
        """Multiple worker sets each with their own image."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "image": {
                            "repository": "base/airflow",
                            "tag": "base",
                        },
                        "sets": [
                            {
                                "name": "set1",
                                "queue": "q1",
                                "image": {
                                    "repository": "set1/airflow",
                                    "tag": "v1",
                                },
                            },
                            {
                                "name": "set2",
                                "queue": "q2",
                                "image": {
                                    "repository": "set2/airflow",
                                    "tag": "v2",
                                },
                            },
                            {
                                "name": "set3-no-override",
                                "queue": "q3",
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert len(docs) == 3

        set1_images = _all_airflow_images(docs[0])
        for image in set1_images:
            assert image == "set1/airflow:v1"

        set2_images = _all_airflow_images(docs[1])
        for image in set2_images:
            assert image == "set2/airflow:v2"

        # set3 has no image override, inherits from celery level
        set3_images = _all_airflow_images(docs[2])
        for image in set3_images:
            assert image == "base/airflow:base"


class TestWorkerImagePullPolicy:
    """Tests for image pull policy overrides."""

    def test_default_pull_policy(self):
        """Default pull policy is IfNotPresent from images.airflow.pullPolicy."""
        docs = render_chart(
            values={"executor": "CeleryExecutor"},
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        policies = _all_airflow_pull_policies(docs[0])
        for policy in policies:
            assert policy == "IfNotPresent"

    def test_celery_pull_policy_override(self):
        """workers.celery.image.pullPolicy overrides the default."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "custom/airflow",
                            "tag": "latest",
                            "pullPolicy": "Always",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        policies = _all_airflow_pull_policies(docs[0])
        for policy in policies:
            assert policy == "Always"

    def test_per_set_pull_policy_override(self):
        """Per-set image.pullPolicy overrides celery-level pullPolicy."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": True,
                        "image": {
                            "repository": "base/airflow",
                            "tag": "v1",
                            "pullPolicy": "Always",
                        },
                        "sets": [
                            {
                                "name": "special",
                                "queue": "special",
                                "image": {
                                    "repository": "special/airflow",
                                    "tag": "v2",
                                    "pullPolicy": "Never",
                                },
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert len(docs) == 2

        # Default set uses celery-level pullPolicy
        default_policies = _all_airflow_pull_policies(docs[0])
        for policy in default_policies:
            assert policy == "Always"

        # Named set uses its own pullPolicy
        special_policies = _all_airflow_pull_policies(docs[1])
        for policy in special_policies:
            assert policy == "Never"


class TestWorkerImageForMigrations:
    """Tests for wait-for-migrations init container image behavior."""

    def test_migrations_uses_worker_image_by_default(self):
        """wait-for-migrations uses the worker image when useDefaultImageForMigration is false."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "custom/airflow",
                            "tag": "custom-v1",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []
        migration_containers = [c for c in init_containers if c["name"] == "wait-for-airflow-migrations"]
        assert len(migration_containers) == 1
        assert migration_containers[0]["image"] == "custom/airflow:custom-v1"

    def test_migrations_uses_default_image_when_flag_set(self):
        """wait-for-migrations uses defaultAirflow image when useDefaultImageForMigration is true."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "images": {
                    "useDefaultImageForMigration": True,
                },
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "custom/airflow",
                            "tag": "custom-v1",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []
        migration_containers = [c for c in init_containers if c["name"] == "wait-for-airflow-migrations"]
        assert len(migration_containers) == 1
        # Should use the default image, not the worker override
        assert migration_containers[0]["image"] == DEFAULT_AIRFLOW_IMAGE

    def test_migrations_per_set_uses_set_image(self):
        """wait-for-migrations uses the per-set image when useDefaultImageForMigration is false."""
        docs = render_chart(
            name="test",
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "image": {
                            "repository": "celery-base/airflow",
                            "tag": "base-v1",
                        },
                        "sets": [
                            {
                                "name": "gpu",
                                "queue": "gpu",
                                "image": {
                                    "repository": "gpu/airflow",
                                    "tag": "cuda-v1",
                                },
                            },
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []
        migration_containers = [c for c in init_containers if c["name"] == "wait-for-airflow-migrations"]
        assert len(migration_containers) == 1
        assert migration_containers[0]["image"] == "gpu/airflow:cuda-v1"


class TestWorkerImageContainerConsistency:
    """Tests that ALL worker containers consistently use the worker image."""

    @pytest.mark.parametrize(
        "container_name",
        ["worker", "worker-log-groomer"],
    )
    def test_main_containers_use_worker_image(self, container_name):
        """Main worker containers use the worker image override."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "custom/airflow",
                            "tag": "worker-v1",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        containers = jmespath.search("spec.template.spec.containers", docs[0])
        matched = [c for c in containers if c["name"] == container_name]
        assert len(matched) == 1, f"Container {container_name} not found"
        assert matched[0]["image"] == "custom/airflow:worker-v1"

    def test_kerberos_containers_use_worker_image(self):
        """Kerberos init and sidecar containers use the worker image override."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "kerberosInitContainer": {
                        "enabled": True,
                    },
                    "kerberosSidecar": {
                        "enabled": True,
                    },
                    "celery": {
                        "image": {
                            "repository": "custom/airflow",
                            "tag": "worker-v1",
                        },
                    },
                },
                "kerberos": {
                    "enabled": True,
                    "keytabBase64Content": "dGVzdA==",
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        all_containers = (
            (jmespath.search("spec.template.spec.initContainers", docs[0]) or [])
            + (jmespath.search("spec.template.spec.containers", docs[0]) or [])
        )
        kerberos_containers = [
            c
            for c in all_containers
            if c["name"] in ("kerberos-init", "worker-kerberos")
        ]
        assert len(kerberos_containers) > 0, "No kerberos containers found"
        for container in kerberos_containers:
            assert container["image"] == "custom/airflow:worker-v1", (
                f"Container {container['name']} has wrong image: {container['image']}"
            )


class TestWorkerImageCeleryKubernetesExecutor:
    """Tests that image overrides work with CeleryKubernetesExecutor."""

    def test_celery_image_with_celery_kubernetes_executor(self):
        """Image overrides work with CeleryKubernetesExecutor."""
        docs = render_chart(
            values={
                "executor": "CeleryKubernetesExecutor",
                "workers": {
                    "celery": {
                        "image": {
                            "repository": "celery-custom/airflow",
                            "tag": "celery-v1",
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        images = _all_airflow_images(docs[0])
        assert len(images) > 0
        for image in images:
            assert image == "celery-custom/airflow:celery-v1"
