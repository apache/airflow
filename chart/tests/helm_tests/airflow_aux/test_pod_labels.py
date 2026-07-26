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

import pytest
from chart_utils.helm_template_generator import render_chart

# Components whose workload object carries only the global labels until
# ``podLabels`` is set. ``dagProcessor`` has its own dedicated module in
# ``helm_tests/dagprocessor/test_labels_deployment.py``.
COMPONENTS = [
    ("apiServer", "templates/api-server/api-server-deployment.yaml", {}),
    ("scheduler", "templates/scheduler/scheduler-deployment.yaml", {}),
    (
        "workers.celery",
        "templates/workers/worker-deployment.yaml",
        {"executor": "CeleryExecutor"},
    ),
    ("triggerer", "templates/triggerer/triggerer-deployment.yaml", {}),
    (
        "flower",
        "templates/flower/flower-deployment.yaml",
        {"executor": "CeleryExecutor", "flower": {"enabled": True}},
    ),
    ("statsd", "templates/statsd/statsd-deployment.yaml", {}),
    (
        "pgbouncer",
        "templates/pgbouncer/pgbouncer-deployment.yaml",
        {"pgbouncer": {"enabled": True}},
    ),
    ("redis", "templates/redis/redis-statefulset.yaml", {"executor": "CeleryExecutor"}),
]

GLOBAL_LABELS = {"test_global_label": "test_global_label_value", "common_label": "global_value"}
COMPONENT_LABELS = {
    "test_component_label": "test_component_label_value",
    "common_label": "component_value",
}
POD_LABELS = {"test_pod_label": "test_pod_label_value", "common_label": "pod_value"}
OTEL_ENABLED = {"otelCollector": {"tracesEnabled": True}}


def _deep_merge(base: dict, overlay: dict) -> dict:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _nest(values_key: str, payload: dict) -> dict:
    """Expand a dotted values key, so ``workers.celery`` reaches the right table."""
    for part in reversed(values_key.split(".")):
        payload = {part: payload}
    return payload


def _render(values_key: str, show_only: str, extra_values: dict, component: dict) -> tuple[dict, dict]:
    values = _deep_merge({"labels": GLOBAL_LABELS}, extra_values)
    values = _deep_merge(values, _nest(values_key, component))
    docs = render_chart(values=values, show_only=[show_only])
    return docs[0]["metadata"]["labels"], docs[0]["spec"]["template"]["metadata"]["labels"]


@pytest.mark.parametrize(("values_key", "show_only", "extra_values"), COMPONENTS)
class TestPodLabels:
    """Tests separating workload object labels from pod labels."""

    def test_should_keep_component_labels_on_pods_when_pod_labels_are_unset(
        self, values_key, show_only, extra_values
    ):
        object_labels, pod_labels = _render(values_key, show_only, extra_values, {"labels": COMPONENT_LABELS})

        # Backward compatible: component labels reach pods, not the workload object.
        assert object_labels["test_global_label"] == "test_global_label_value"
        assert "test_component_label" not in object_labels
        assert object_labels["common_label"] == "global_value"
        assert pod_labels["test_global_label"] == "test_global_label_value"
        assert pod_labels["test_component_label"] == "test_component_label_value"
        assert pod_labels["common_label"] == "component_value"

    def test_should_separate_object_and_pod_labels(self, values_key, show_only, extra_values):
        object_labels, pod_labels = _render(
            values_key,
            show_only,
            extra_values,
            {"labels": COMPONENT_LABELS, "podLabels": POD_LABELS},
        )

        assert object_labels["test_global_label"] == "test_global_label_value"
        assert object_labels["test_component_label"] == "test_component_label_value"
        assert "test_pod_label" not in object_labels
        assert object_labels["common_label"] == "component_value"
        assert pod_labels["test_global_label"] == "test_global_label_value"
        assert "test_component_label" not in pod_labels
        assert pod_labels["test_pod_label"] == "test_pod_label_value"
        assert pod_labels["common_label"] == "pod_value"

    def test_should_treat_empty_pod_labels_as_separate(self, values_key, show_only, extra_values):
        object_labels, pod_labels = _render(
            values_key, show_only, extra_values, {"labels": COMPONENT_LABELS, "podLabels": {}}
        )

        assert object_labels["test_component_label"] == "test_component_label_value"
        assert "test_component_label" not in pod_labels
        assert pod_labels["test_global_label"] == "test_global_label_value"
        assert pod_labels["common_label"] == "global_value"


class TestOtelCollectorPodLabels:
    """OTel Collector already merges component labels onto its Deployment."""

    SHOW_ONLY = "templates/otel-collector/otel-collector-deployment.yaml"

    def test_should_keep_component_labels_on_both_when_pod_labels_are_unset(self):
        object_labels, pod_labels = _render(
            "otelCollector", self.SHOW_ONLY, OTEL_ENABLED, {"labels": COMPONENT_LABELS}
        )

        assert object_labels["test_component_label"] == "test_component_label_value"
        assert pod_labels["test_component_label"] == "test_component_label_value"
        assert object_labels["common_label"] == "component_value"
        assert pod_labels["common_label"] == "component_value"

    def test_should_separate_object_and_pod_labels(self):
        object_labels, pod_labels = _render(
            "otelCollector",
            self.SHOW_ONLY,
            OTEL_ENABLED,
            {"labels": COMPONENT_LABELS, "podLabels": POD_LABELS},
        )

        assert object_labels["test_component_label"] == "test_component_label_value"
        assert "test_pod_label" not in object_labels
        assert "test_component_label" not in pod_labels
        assert pod_labels["test_pod_label"] == "test_pod_label_value"
        assert pod_labels["common_label"] == "pod_value"

    def test_should_treat_empty_pod_labels_as_separate(self):
        object_labels, pod_labels = _render(
            "otelCollector",
            self.SHOW_ONLY,
            OTEL_ENABLED,
            {"labels": COMPONENT_LABELS, "podLabels": {}},
        )

        assert object_labels["test_component_label"] == "test_component_label_value"
        assert "test_component_label" not in pod_labels
        assert pod_labels["test_global_label"] == "test_global_label_value"
