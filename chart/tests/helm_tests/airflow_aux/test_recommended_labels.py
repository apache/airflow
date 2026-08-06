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

from collections.abc import Iterator
from pathlib import Path
from shutil import copyfile, copytree
from typing import Any

import pytest
import yaml
from chart_utils.helm_template_generator import prepare_k8s_lookup_dict, render_chart

AIRFLOW_ROOT = Path(__file__).parents[4]
CHART_DIR = AIRFLOW_ROOT / "chart"
RELEASE_NAME = "recommended-labels"
LONG_CHART_VERSION = "2.0.0+" + "build" * 20

CHART_METADATA = yaml.safe_load((CHART_DIR / "Chart.yaml").read_text())
CHART_LABEL = f"{CHART_METADATA['name']}-{CHART_METADATA['version']}".replace("+", "_")[:63].strip("-_.")

RESOURCE_STANDARD_LABELS = {
    "app.kubernetes.io/instance": RELEASE_NAME,
    "app.kubernetes.io/managed-by": "Helm",
    "app.kubernetes.io/part-of": "airflow",
    "helm.sh/chart": CHART_LABEL,
}
POD_STANDARD_LABELS = {
    "app.kubernetes.io/instance": RELEASE_NAME,
    "app.kubernetes.io/part-of": "airflow",
}
OUT_OF_SCOPE_LABELS = {"app.kubernetes.io/name", "app.kubernetes.io/version"}


@pytest.fixture(scope="module")
def representative_objects() -> list[dict[str, Any]]:
    return render_chart(
        name=RELEASE_NAME,
        values={
            "executor": "CeleryExecutor,KubernetesExecutor",
            "apiServer": {"podDisruptionBudget": {"enabled": True}},
            "cleanup": {"enabled": True},
            "dagProcessor": {"podDisruptionBudget": {"enabled": True}},
            "dags": {"persistence": {"enabled": True}},
            "databaseCleanup": {"enabled": True},
            "flower": {"enabled": True},
            "logs": {"persistence": {"enabled": True}},
            "networkPolicies": {"enabled": True},
            "otelCollector": {"tracesEnabled": True},
            "pgbouncer": {"enabled": True},
            "postgresql": {"enabled": False},
            "scheduler": {"podDisruptionBudget": {"enabled": True}},
            "triggerer": {"podDisruptionBudget": {"enabled": True}},
            "workers": {"celery": {"podDisruptionBudget": {"enabled": True}}},
        },
    )


@pytest.fixture(scope="module")
def label_probe_chart(tmp_path_factory: pytest.TempPathFactory) -> Path:
    temp_chart_dir = tmp_path_factory.mktemp("recommended-labels") / "chart"
    copytree(CHART_DIR, temp_chart_dir)
    chart_file = temp_chart_dir / "Chart.yaml"
    chart_file.write_text(
        chart_file.read_text().replace("version: 2.0.0", f"version: {LONG_CHART_VERSION}", 1)
    )
    copyfile(
        temp_chart_dir / "files/pod-template-file.kubernetes-helm-yaml",
        temp_chart_dir / "templates/pod-template-file.yaml",
    )
    (temp_chart_dir / "templates/label-probe.yaml").write_text(
        """\
apiVersion: v1
kind: ConfigMap
metadata:
  name: label-probe-with-component
  labels:
    {{- include "airflow.standardLabels" (dict "root" . "component" .Values.labelProbeComponent) | nindent 4 }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: label-probe-without-component
  labels:
    {{- include "airflow.standardLabels" (dict "root" .) | nindent 4 }}
---
apiVersion: v1
kind: Pod
metadata:
  name: label-probe-pod
  labels:
    {{- include "airflow.standardPodLabels" (dict "root" . "component" .Values.labelProbeComponent) | nindent 4 }}
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: busybox
"""
    )
    return temp_chart_dir


def _iter_pod_templates(objects: list[dict[str, Any]]) -> Iterator[tuple[str, dict[str, Any]]]:
    for obj in objects:
        kind = obj["kind"]
        if kind == "CronJob":
            yield obj["metadata"]["name"], obj["spec"]["jobTemplate"]["spec"]["template"]
        elif kind in {"DaemonSet", "Deployment", "Job", "StatefulSet"}:
            yield obj["metadata"]["name"], obj["spec"]["template"]


def _iter_selectors(value: Any) -> Iterator[Any]:
    if isinstance(value, dict):
        for key, nested_value in value.items():
            if "selector" in key.lower():
                yield nested_value
            yield from _iter_selectors(nested_value)
    elif isinstance(value, list):
        for nested_value in value:
            yield from _iter_selectors(nested_value)


def _iter_keys(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for key, nested_value in value.items():
            yield key
            yield from _iter_keys(nested_value)
    elif isinstance(value, list):
        for nested_value in value:
            yield from _iter_keys(nested_value)


def test_chart_resources_have_additive_recommended_labels(
    representative_objects: list[dict[str, Any]],
):
    assert representative_objects
    for obj in representative_objects:
        labels = obj["metadata"]["labels"]
        object_name = f"{obj['kind']}/{obj['metadata']['name']}"

        assert labels.items() >= RESOURCE_STANDARD_LABELS.items(), object_name
        assert labels["release"] == RELEASE_NAME, object_name
        assert labels["heritage"] == "Helm", object_name
        assert "chart" in labels, object_name
        assert not OUT_OF_SCOPE_LABELS & labels.keys(), object_name

        if component := labels.get("component"):
            assert labels["app.kubernetes.io/component"] == component, object_name
        else:
            assert "app.kubernetes.io/component" not in labels, object_name


def test_chart_pods_have_only_pod_scoped_recommended_labels(
    representative_objects: list[dict[str, Any]],
):
    pod_templates = list(_iter_pod_templates(representative_objects))

    assert pod_templates
    for object_name, pod_template in pod_templates:
        labels = pod_template["metadata"]["labels"]

        assert labels.items() >= POD_STANDARD_LABELS.items(), object_name
        assert labels["app.kubernetes.io/component"] == labels["component"], object_name
        assert "app.kubernetes.io/managed-by" not in labels, object_name
        assert "helm.sh/chart" not in labels, object_name
        assert not OUT_OF_SCOPE_LABELS & labels.keys(), object_name


def test_recommended_labels_are_not_added_to_selectors(
    representative_objects: list[dict[str, Any]],
):
    standard_label_keys = RESOURCE_STANDARD_LABELS.keys() | {"app.kubernetes.io/component"}

    for obj in representative_objects:
        object_name = f"{obj['kind']}/{obj['metadata']['name']}"
        for selector in _iter_selectors(obj.get("spec", {})):
            assert not standard_label_keys & set(_iter_keys(selector)), object_name
        if obj["kind"] in {"Deployment", "StatefulSet"}:
            selector_labels = obj["spec"]["selector"]["matchLabels"]
            pod_labels = obj["spec"]["template"]["metadata"]["labels"]
            assert pod_labels.items() >= selector_labels.items(), object_name

    objects_by_key = prepare_k8s_lookup_dict(representative_objects)
    expected_scheduler_selector = {
        "tier": "airflow",
        "component": "scheduler",
        "release": RELEASE_NAME,
    }
    assert objects_by_key[("Deployment", f"{RELEASE_NAME}-scheduler")]["spec"]["selector"] == {
        "matchLabels": expected_scheduler_selector
    }
    assert objects_by_key[("PodDisruptionBudget", f"{RELEASE_NAME}-scheduler-pdb")]["spec"]["selector"] == {
        "matchLabels": expected_scheduler_selector
    }
    assert objects_by_key[("NetworkPolicy", f"{RELEASE_NAME}-scheduler-policy")]["spec"]["podSelector"] == {
        "matchLabels": expected_scheduler_selector
    }


def test_user_labels_keep_precedence_over_recommended_defaults():
    user_labels = {
        "app.kubernetes.io/component": "custom-component",
        "app.kubernetes.io/instance": "custom-instance",
        "app.kubernetes.io/managed-by": "custom-manager",
        "app.kubernetes.io/part-of": "custom-application",
        "helm.sh/chart": "custom-chart",
    }
    objects = render_chart(
        name=RELEASE_NAME,
        values={"labels": user_labels, "postgresql": {"enabled": False}},
        show_only=[
            "templates/configmaps/configmap.yaml",
            "templates/scheduler/scheduler-deployment.yaml",
        ],
    )
    objects_by_key = prepare_k8s_lookup_dict(objects)

    configmap_labels = objects_by_key[("ConfigMap", f"{RELEASE_NAME}-config")]["metadata"]["labels"]
    scheduler_labels = objects_by_key[("Deployment", f"{RELEASE_NAME}-scheduler")]["spec"]["template"][
        "metadata"
    ]["labels"]
    assert configmap_labels.items() >= user_labels.items()
    assert scheduler_labels.items() >= user_labels.items()


@pytest.mark.parametrize(
    ("component", "expected_component"),
    [
        pytest.param("Scheduler.Worker_", "Scheduler.Worker", id="preserve-case-and-trim"),
        pytest.param("A" * 70, "A" * 63, id="truncate"),
    ],
)
def test_label_helpers_sanitize_label_values(
    label_probe_chart: Path,
    component: str,
    expected_component: str,
):
    objects = render_chart(
        name=RELEASE_NAME,
        values={"labelProbeComponent": component},
        show_only=["templates/label-probe.yaml"],
        chart_dir=label_probe_chart.as_posix(),
    )
    objects_by_key = prepare_k8s_lookup_dict(objects)
    resource_labels = objects_by_key[("ConfigMap", "label-probe-with-component")]["metadata"]["labels"]
    labels_without_component = objects_by_key[("ConfigMap", "label-probe-without-component")]["metadata"][
        "labels"
    ]
    pod_labels = objects_by_key[("Pod", "label-probe-pod")]["metadata"]["labels"]
    expected_chart_label = f"airflow-{LONG_CHART_VERSION}".replace("+", "_")[:63].strip("-_.")

    assert resource_labels["app.kubernetes.io/component"] == expected_component
    assert pod_labels["app.kubernetes.io/component"] == expected_component
    assert "app.kubernetes.io/component" not in labels_without_component
    assert resource_labels["helm.sh/chart"] == expected_chart_label
    assert len(resource_labels["helm.sh/chart"]) <= 63
    assert "+" not in resource_labels["helm.sh/chart"]


def test_kubernetes_executor_pod_template_has_pod_scoped_labels(label_probe_chart: Path):
    objects = render_chart(
        name=RELEASE_NAME,
        values={"labelProbeComponent": "probe"},
        show_only=["templates/pod-template-file.yaml"],
        chart_dir=label_probe_chart.as_posix(),
    )
    labels = objects[0]["metadata"]["labels"]

    assert labels.items() >= POD_STANDARD_LABELS.items()
    assert labels["app.kubernetes.io/component"] == labels["component"] == "worker"
    assert "app.kubernetes.io/managed-by" not in labels
    assert "helm.sh/chart" not in labels
