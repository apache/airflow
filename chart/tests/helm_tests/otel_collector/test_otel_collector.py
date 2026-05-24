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
from chart_utils.helm_template_generator import render_chart

OTEL_TEMPLATES = [
    "templates/configmaps/otel-collector-configmap.yaml",
    "templates/otel-collector/otel-collector-deployment.yaml",
    "templates/otel-collector/otel-collector-service.yaml",
    "templates/otel-collector/otel-collector-serviceaccount.yaml",
]

DEPLOYMENT_TEMPLATE = "templates/otel-collector/otel-collector-deployment.yaml"
SERVICE_TEMPLATE = "templates/otel-collector/otel-collector-service.yaml"
CONFIGMAP_TEMPLATE = "templates/configmaps/otel-collector-configmap.yaml"
SERVICE_ACCOUNT_TEMPLATE = "templates/otel-collector/otel-collector-serviceaccount.yaml"
AIRFLOW_CONFIGMAP_TEMPLATE = "templates/configmaps/configmap.yaml"

AIRFLOW_POD_TEMPLATES = [
    "templates/api-server/api-server-deployment.yaml",
    "templates/scheduler/scheduler-deployment.yaml",
    "templates/workers/worker-deployment.yaml",
    "templates/triggerer/triggerer-deployment.yaml",
    "templates/dag-processor/dag-processor-deployment.yaml",
]


def _env_names(doc):
    """Return the list of env var names from the first container of a pod spec."""
    env = jmespath.search("spec.template.spec.containers[0].env", doc) or []
    return [e["name"] for e in env]


def _env_value(doc, name):
    env = jmespath.search("spec.template.spec.containers[0].env", doc) or []
    for entry in env:
        if entry["name"] == name:
            return entry.get("value")
    return None


class TestOtelCollectorResourceGating:
    """Resource emission depends on tracesEnabled / metricsEnabled."""

    def test_default_renders_all_resources(self):
        docs = render_chart(show_only=OTEL_TEMPLATES)
        kinds = {d["kind"] for d in docs}
        assert kinds == {"ConfigMap", "Deployment", "Service", "ServiceAccount"}

    def test_both_flags_disabled_renders_nothing(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": False, "metricsEnabled": False}},
            show_only=OTEL_TEMPLATES,
        )
        assert docs == []

    def test_metrics_only_renders_all_resources(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": False, "metricsEnabled": True}},
            show_only=OTEL_TEMPLATES,
        )
        kinds = {d["kind"] for d in docs}
        assert kinds == {"ConfigMap", "Deployment", "Service", "ServiceAccount"}


class TestOtelCollectorDefaultConfig:
    """The chart-rendered config.yml has the default config for the otel-collector."""

    @staticmethod
    def _get_config_yml(values=None):
        docs = render_chart(values=values or {}, show_only=[CONFIGMAP_TEMPLATE])
        config_yml = jmespath.search('data."config.yml"', docs[0])
        return yaml.safe_load(config_yml)

    def test_health_check_extension(self):
        config = self._get_config_yml()
        assert config["extensions"]["health_check"]["endpoint"] == "0.0.0.0:13133"

    def test_otlp_receivers_on_default_ports(self):
        config = self._get_config_yml()
        protocols = config["receivers"]["otlp"]["protocols"]
        assert protocols["grpc"]["endpoint"] == "0.0.0.0:4317"
        assert protocols["http"]["endpoint"] == "0.0.0.0:4318"

    def test_default_exporters_are_logging_only(self):
        config = self._get_config_yml()
        assert set(config["exporters"].keys()) == {"logging"}

    def test_default_pipelines_only_export_to_logging(self):
        config = self._get_config_yml()
        pipelines = config["service"]["pipelines"]
        assert pipelines["traces"]["exporters"] == ["logging"]
        assert pipelines["metrics"]["exporters"] == ["logging"]


class TestOtelCollectorConfigOverride:
    """`otelCollector.config` replaces the default and supports `tpl`."""

    def test_override_replaces_default(self):
        override = (
            "receivers:\n"
            "  otlp:\n"
            "    protocols:\n"
            "      http:\n"
            "        endpoint: 0.0.0.0:9999\n"
            "exporters:\n"
            "  logging: {}\n"
            "service:\n"
            "  pipelines:\n"
            "    traces:\n"
            "      receivers: [otlp]\n"
            "      exporters: [logging]\n"
        )
        docs = render_chart(
            values={"otelCollector": {"config": override}},
            show_only=[CONFIGMAP_TEMPLATE],
        )
        rendered = yaml.safe_load(jmespath.search('data."config.yml"', docs[0]))
        # The value is '0.0.0.0:9999' instead of the default '0.0.0.0:4318'.
        assert rendered["receivers"]["otlp"]["protocols"]["http"]["endpoint"] == "0.0.0.0:9999"
        # default `health_check` extension is gone in the override
        assert "extensions" not in rendered

    def test_tpl_resolves_chart_values(self):
        override = (
            "receivers:\n"
            "  otlp:\n"
            "    protocols:\n"
            "      http:\n"
            "        endpoint: 0.0.0.0:{{ .Values.ports.otelCollectorOtlpHttp }}\n"
        )
        docs = render_chart(
            values={"otelCollector": {"config": override}},
            show_only=[CONFIGMAP_TEMPLATE],
        )
        rendered = yaml.safe_load(jmespath.search('data."config.yml"', docs[0]))
        assert rendered["receivers"]["otlp"]["protocols"]["http"]["endpoint"] == "0.0.0.0:4318"


class TestOtelCollectorDeployment:
    """Deployment-level configurability."""

    def test_default_args(self):
        docs = render_chart(show_only=[DEPLOYMENT_TEMPLATE])
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "--config=/etc/otel-collector/config.yml"
        ]

    def test_args_override(self):
        custom = ["--config=/etc/otel-collector/config.yml", "--feature-gates=+foo"]
        docs = render_chart(
            values={"otelCollector": {"args": custom}},
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == custom

    def test_default_probes(self):
        docs = render_chart(show_only=[DEPLOYMENT_TEMPLATE])
        liveness = jmespath.search("spec.template.spec.containers[0].livenessProbe", docs[0])
        readiness = jmespath.search("spec.template.spec.containers[0].readinessProbe", docs[0])
        assert liveness["initialDelaySeconds"] == 10
        assert liveness["periodSeconds"] == 15
        assert liveness["httpGet"] == {"path": "/", "port": 13133}
        assert readiness["initialDelaySeconds"] == 10
        assert readiness["periodSeconds"] == 15

    def test_probe_overrides(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "livenessProbe": {"initialDelaySeconds": 30, "periodSeconds": 45},
                    "readinessProbe": {"initialDelaySeconds": 5, "periodSeconds": 7},
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        liveness = jmespath.search("spec.template.spec.containers[0].livenessProbe", docs[0])
        readiness = jmespath.search("spec.template.spec.containers[0].readinessProbe", docs[0])
        assert liveness["initialDelaySeconds"] == 30
        assert liveness["periodSeconds"] == 45
        assert readiness["initialDelaySeconds"] == 5
        assert readiness["periodSeconds"] == 7

    def test_resources_default_empty(self):
        docs = render_chart(show_only=[DEPLOYMENT_TEMPLATE])
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    def test_resources_configurable(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "256Mi"},
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                    }
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        resources = jmespath.search("spec.template.spec.containers[0].resources", docs[0])
        assert resources == {
            "limits": {"cpu": "200m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "128Mi"},
        }

    @pytest.mark.parametrize(
        ("values", "expected"),
        [
            ({}, 30),
            ({"otelCollector": {"terminationGracePeriodSeconds": 1200}}, 1200),
        ],
    )
    def test_termination_grace_period_seconds(self, values, expected):
        docs = render_chart(values=values, show_only=[DEPLOYMENT_TEMPLATE])
        assert jmespath.search("spec.template.spec.terminationGracePeriodSeconds", docs[0]) == expected

    @pytest.mark.parametrize(
        ("component", "global_", "expected"),
        [(8, 10, 8), (10, 8, 10), (8, None, 8), (None, 10, 10), (0, 10, 0), (None, 0, 0)],
    )
    def test_revision_history_limit(self, component, global_, expected):
        values = {"otelCollector": {}}
        if component is not None:
            values["otelCollector"]["revisionHistoryLimit"] = component
        if global_ is not None:
            values["revisionHistoryLimit"] = global_
        docs = render_chart(values=values, show_only=[DEPLOYMENT_TEMPLATE])
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected

    def test_scheduling_constraints(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "nodeSelector": {"diskType": "ssd"},
                    "tolerations": [{"key": "k", "operator": "Equal", "value": "v", "effect": "NoSchedule"}],
                    "affinity": {
                        "nodeAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [
                                    {
                                        "matchExpressions": [
                                            {"key": "foo", "operator": "In", "values": ["true"]}
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    "topologySpreadConstraints": [
                        {"maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule"}
                    ],
                    "priorityClassName": "high-priority",
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        spec = jmespath.search("spec.template.spec", docs[0])
        assert spec["nodeSelector"] == {"diskType": "ssd"}
        assert spec["tolerations"][0]["key"] == "k"
        assert (
            spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][
                "nodeSelectorTerms"
            ][0]["matchExpressions"][0]["key"]
            == "foo"
        )
        assert spec["topologySpreadConstraints"][0]["topologyKey"] == "zone"
        assert spec["priorityClassName"] == "high-priority"

    def test_security_contexts(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "securityContexts": {
                        "pod": {"runAsUser": 2000, "fsGroup": 1000},
                        "container": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True},
                    }
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert jmespath.search("spec.template.spec.securityContext", docs[0]) == {
            "runAsUser": 2000,
            "fsGroup": 1000,
        }
        assert jmespath.search("spec.template.spec.containers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
        }

    def test_annotations_and_pod_annotations(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "annotations": {"deploy_anno": "deploy_value"},
                    "podAnnotations": {"pod_anno": "pod_value"},
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert jmespath.search("metadata.annotations.deploy_anno", docs[0]) == "deploy_value"
        assert jmespath.search("spec.template.metadata.annotations.pod_anno", docs[0]) == "pod_value"

    def test_image_override(self):
        docs = render_chart(
            values={
                "images": {
                    "otelCollector": {
                        "repository": "example/custom-otel",
                        "tag": "1.2.3",
                        "pullPolicy": "Always",
                    }
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].image", docs[0]) == "example/custom-otel:1.2.3"
        )
        assert jmespath.search("spec.template.spec.containers[0].imagePullPolicy", docs[0]) == "Always"


class TestOtelCollectorService:
    """Service-level configurability."""

    def test_service_annotations(self):
        docs = render_chart(
            values={"otelCollector": {"service": {"annotations": {"some_anno": "some_value"}}}},
            show_only=[SERVICE_TEMPLATE],
        )
        assert jmespath.search("metadata.annotations.some_anno", docs[0]) == "some_value"

    def test_service_no_annotations_block_when_unset(self):
        docs = render_chart(show_only=[SERVICE_TEMPLATE])
        assert "annotations" not in jmespath.search("metadata", docs[0])


class TestOtelCollectorServiceAccount:
    """ServiceAccount-level configurability."""

    def test_default_create(self):
        docs = render_chart(show_only=[SERVICE_ACCOUNT_TEMPLATE])
        assert docs[0]["kind"] == "ServiceAccount"
        assert docs[0]["automountServiceAccountToken"] is False

    def test_create_false_suppresses_sa(self):
        docs = render_chart(
            values={"otelCollector": {"serviceAccount": {"create": False}}},
            show_only=[SERVICE_ACCOUNT_TEMPLATE],
        )
        assert docs == []

    def test_create_false_deployment_uses_default_sa(self):
        docs = render_chart(
            values={"otelCollector": {"serviceAccount": {"create": False}}},
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert jmespath.search("spec.template.spec.serviceAccountName", docs[0]) == "default"

    def test_custom_sa_name_propagates_to_deployment(self):
        docs = render_chart(
            values={"otelCollector": {"serviceAccount": {"name": "my-custom-sa"}}},
            show_only=[DEPLOYMENT_TEMPLATE, SERVICE_ACCOUNT_TEMPLATE],
        )
        sa = next(d for d in docs if d["kind"] == "ServiceAccount")
        deployment = next(d for d in docs if d["kind"] == "Deployment")
        assert sa["metadata"]["name"] == "my-custom-sa"
        assert jmespath.search("spec.template.spec.serviceAccountName", deployment) == "my-custom-sa"

    def test_automount_token_override(self):
        docs = render_chart(
            values={"otelCollector": {"serviceAccount": {"automountServiceAccountToken": True}}},
            show_only=[SERVICE_ACCOUNT_TEMPLATE],
        )
        assert docs[0]["automountServiceAccountToken"] is True

    def test_sa_annotations(self):
        docs = render_chart(
            values={"otelCollector": {"serviceAccount": {"annotations": {"sa_anno": "sa_value"}}}},
            show_only=[SERVICE_ACCOUNT_TEMPLATE],
        )
        assert jmespath.search("metadata.annotations.sa_anno", docs[0]) == "sa_value"


class TestOtelCollectorAirflowEnvironment:
    """The correct OTEL_* env vars can be found on Airflow pods based on the trace/metrics flags."""

    @pytest.mark.parametrize("template", AIRFLOW_POD_TEMPLATES)
    def test_default_emits_traces_env_only(self, template):
        docs = render_chart(show_only=[template])
        names = _env_names(docs[0])
        assert "OTEL_SERVICE_NAME" in names
        assert "OTEL_EXPORTER_OTLP_PROTOCOL" in names
        assert "OTEL_TRACES_EXPORTER" in names
        assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" in names
        # Metrics env vars aren't present.
        assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" not in names
        assert "OTEL_METRIC_EXPORT_INTERVAL" not in names

    @pytest.mark.parametrize("template", AIRFLOW_POD_TEMPLATES)
    def test_metrics_enabled_emits_metrics_env(self, template):
        docs = render_chart(
            values={"otelCollector": {"metricsEnabled": True}},
            show_only=[template],
        )
        names = _env_names(docs[0])
        assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" in names
        assert "OTEL_METRIC_EXPORT_INTERVAL" in names

    @pytest.mark.parametrize("template", AIRFLOW_POD_TEMPLATES)
    def test_both_flags_off_emits_no_otel_env(self, template):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": False, "metricsEnabled": False}},
            show_only=[template],
        )
        names = _env_names(docs[0])
        assert not any(n.startswith("OTEL_") for n in names)

    def test_metric_export_interval_value(self):
        docs = render_chart(
            values={"otelCollector": {"metricsEnabled": True, "metricExportIntervalMs": 12345}},
            show_only=[AIRFLOW_POD_TEMPLATES[0]],
        )
        assert _env_value(docs[0], "OTEL_METRIC_EXPORT_INTERVAL") == "12345"


class TestOtelCollectorAirflowConfig:
    """Renders the correct values in airflow.cfg."""

    @staticmethod
    def _get_conf_section(cfg: str, name: str) -> str:
        """Return the body of the specified section (everything until the next '[')."""
        return cfg.split(f"[{name}]")[1].split("[")[0]

    @staticmethod
    def _get_airflow_conf(values=None):
        docs = render_chart(values=values or {}, show_only=[AIRFLOW_CONFIGMAP_TEMPLATE])
        return jmespath.search('data."airflow.cfg"', docs[0])

    def test_default_traces_section_has_otel_on_true(self):
        traces = self._get_conf_section(self._get_airflow_conf(), "traces")
        assert "otel_on = True" in traces

    def test_default_metrics_section_has_otel_off_and_statsd_on(self):
        metrics = self._get_conf_section(self._get_airflow_conf(), "metrics")
        assert "otel_on = False" in metrics
        assert "statsd_on = True" in metrics

    def test_traces_disabled_otel_off(self):
        traces = self._get_conf_section(
            self._get_airflow_conf({"otelCollector": {"tracesEnabled": False}}), "traces"
        )
        assert "otel_on = False" in traces

    def test_metrics_enabled_disables_statsd_and_enables_otel(self):
        metrics = self._get_conf_section(
            self._get_airflow_conf({"otelCollector": {"metricsEnabled": True}}), "metrics"
        )
        assert "statsd_on = False" in metrics
        assert "otel_on = True" in metrics

    def test_metrics_enabled_with_statsd_disabled_still_disables_statsd(self):
        cfg = self._get_airflow_conf(
            {"statsd": {"enabled": False}, "otelCollector": {"metricsEnabled": True}}
        )
        metrics_section = cfg.split("[metrics]")[1].split("[")[0]
        assert "statsd_on = False" in metrics_section
