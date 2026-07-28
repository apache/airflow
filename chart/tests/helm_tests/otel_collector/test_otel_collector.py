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
    (CONFIGMAP_TEMPLATE := "templates/configmaps/otel-collector-configmap.yaml"),
    (DEPLOYMENT_TEMPLATE := "templates/otel-collector/otel-collector-deployment.yaml"),
    (SERVICE_TEMPLATE := "templates/otel-collector/otel-collector-service.yaml"),
    (SERVICE_ACCOUNT_TEMPLATE := "templates/otel-collector/otel-collector-serviceaccount.yaml"),
]

AIRFLOW_CONFIGMAP_TEMPLATE = "templates/configmaps/configmap.yaml"

AIRFLOW_POD_TEMPLATES = [
    "templates/api-server/api-server-deployment.yaml",
    "templates/scheduler/scheduler-deployment.yaml",
    "templates/workers/worker-deployment.yaml",
    "templates/triggerer/triggerer-deployment.yaml",
    "templates/dag-processor/dag-processor-deployment.yaml",
]


class TestOtelCollectorCommon:
    def test_standard_naming(self):
        docs = render_chart(
            name="test-basic",
            values={"useStandardNaming": True, "otelCollector": {"tracesEnabled": True}},
            show_only=OTEL_TEMPLATES,
        )

        assert len(docs) == 4
        assert jmespath.search("[*].[kind, metadata.name]", docs) == [
            ["ConfigMap", "test-basic-airflow-otel-collector"],
            ["Deployment", "test-basic-airflow-otel-collector"],
            ["Service", "test-basic-airflow-otel-collector"],
            ["ServiceAccount", "test-basic-airflow-otel-collector"],
        ]


class TestOtelCollectorResourceGating:
    def test_renders_default(self):
        docs = render_chart(show_only=OTEL_TEMPLATES)
        assert len(docs) == 0

    @pytest.mark.parametrize("otel_field", ["tracesEnabled", "metricsEnabled"])
    def test_should_render(self, otel_field):
        docs = render_chart(values={"otelCollector": {otel_field: True}}, show_only=OTEL_TEMPLATES)

        assert len(docs) == 4
        assert jmespath.search("[*].kind", docs) == ["ConfigMap", "Deployment", "Service", "ServiceAccount"]

    def test_should_not_render(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": False, "metricsEnabled": False}},
            show_only=OTEL_TEMPLATES,
        )

        assert docs == []


class TestOtelCollectorDefaultConfig:
    """The chart-rendered config.yml has the default config for the otel-collector."""

    @staticmethod
    def _get_config_yml():
        docs = render_chart(values={"otelCollector": {"tracesEnabled": True}}, show_only=[CONFIGMAP_TEMPLATE])
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

    def test_override_default(self):
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
            values={"otelCollector": {"tracesEnabled": True, "config": override}},
            show_only=[CONFIGMAP_TEMPLATE],
        )
        rendered = yaml.safe_load(jmespath.search('data."config.yml"', docs[0]))
        assert rendered["receivers"]["otlp"]["protocols"]["http"]["endpoint"] == "0.0.0.0:9999"
        # default `health_check` extension is gone in the override
        assert "extensions" not in rendered

    def test_override_default_tpl(self):
        override = (
            "receivers:\n"
            "  otlp:\n"
            "    protocols:\n"
            "      http:\n"
            "        endpoint: 0.0.0.0:{{ .Values.ports.otelCollectorOtlpHttp }}\n"
        )
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True, "config": override}},
            show_only=[CONFIGMAP_TEMPLATE],
        )
        rendered = yaml.safe_load(jmespath.search('data."config.yml"', docs[0]))
        assert rendered["receivers"]["otlp"]["protocols"]["http"]["endpoint"] == "0.0.0.0:4318"


class TestOtelCollectorDeployment:
    def test_default_args(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True}}, show_only=[DEPLOYMENT_TEMPLATE]
        )
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "--config=/etc/otel-collector/config.yml"
        ]

    def test_args_override(self):
        custom = ["--config=/etc/otel-collector/config.yml", "--feature-gates=+foo"]
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True, "args": custom}},
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == custom

    def test_default_probes(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True}}, show_only=[DEPLOYMENT_TEMPLATE]
        )

        assert jmespath.search("spec.template.spec.containers[0].livenessProbe", docs[0]) == {
            "initialDelaySeconds": 10,
            "periodSeconds": 15,
            "httpGet": {"path": "/", "port": 13133},
        }

        assert jmespath.search("spec.template.spec.containers[0].readinessProbe", docs[0]) == {
            "initialDelaySeconds": 10,
            "periodSeconds": 15,
            "httpGet": {"path": "/", "port": 13133},
        }

    def test_probe_overrides(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "tracesEnabled": True,
                    "livenessProbe": {"initialDelaySeconds": 30, "periodSeconds": 45},
                    "readinessProbe": {"initialDelaySeconds": 5, "periodSeconds": 7},
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.template.spec.containers[0].livenessProbe", docs[0]) == {
            "initialDelaySeconds": 30,
            "periodSeconds": 45,
            "httpGet": {"path": "/", "port": 13133},
        }

        assert jmespath.search("spec.template.spec.containers[0].readinessProbe", docs[0]) == {
            "initialDelaySeconds": 5,
            "periodSeconds": 7,
            "httpGet": {"path": "/", "port": 13133},
        }

    def test_resources_default_empty(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True}}, show_only=[DEPLOYMENT_TEMPLATE]
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    def test_resources_configurable(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "tracesEnabled": True,
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "256Mi"},
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                    },
                }
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {
            "limits": {"cpu": "200m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "128Mi"},
        }

    def test_termination_grace_period_seconds_default(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True}}, show_only=[DEPLOYMENT_TEMPLATE]
        )
        assert jmespath.search("spec.template.spec.terminationGracePeriodSeconds", docs[0]) == 30

    def test_termination_grace_period_seconds_override(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True, "terminationGracePeriodSeconds": 1200}},
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert jmespath.search("spec.template.spec.terminationGracePeriodSeconds", docs[0]) == 1200

    def test_revision_history_limit_default(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True}}, show_only=[DEPLOYMENT_TEMPLATE]
        )
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) is None

    def test_revision_history_limit_global_unset(self):
        docs = render_chart(
            values={"revisionHistoryLimit": None, "otelCollector": {"tracesEnabled": True}},
            show_only=[DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) is None

    def test_revision_history_limit_global(self):
        docs = render_chart(
            values={"revisionHistoryLimit": 8, "otelCollector": {"tracesEnabled": True}},
            show_only=[DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == 8

    @pytest.mark.parametrize(
        ("local_limit", "global_limit"),
        [
            (8, 10),
            (None, 8),
            (8, None),
        ],
    )
    def test_revision_history_limit_overwrite(self, local_limit, global_limit):
        docs = render_chart(
            values={
                "revisionHistoryLimit": global_limit,
                "otelCollector": {"tracesEnabled": True, "revisionHistoryLimit": local_limit},
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )

        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == 8

    def test_scheduling_constraints(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "tracesEnabled": True,
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
                    "tracesEnabled": True,
                    "securityContexts": {
                        "pod": {"runAsUser": 2000, "fsGroup": 1000},
                        "container": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True},
                    },
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
                    "tracesEnabled": True,
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
                },
                "otelCollector": {"tracesEnabled": True},
            },
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].image", docs[0]) == "example/custom-otel:1.2.3"
        )
        assert jmespath.search("spec.template.spec.containers[0].imagePullPolicy", docs[0]) == "Always"


class TestOtelCollectorService:
    def test_service_annotations(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "tracesEnabled": True,
                    "service": {"annotations": {"some_anno": "some_value"}},
                }
            },
            show_only=[SERVICE_TEMPLATE],
        )
        assert jmespath.search("metadata.annotations.some_anno", docs[0]) == "some_value"

    def test_service_no_annotations_block_when_unset(self):
        docs = render_chart(values={"otelCollector": {"tracesEnabled": True}}, show_only=[SERVICE_TEMPLATE])
        assert "annotations" not in jmespath.search("metadata", docs[0])

    def test_ip_family_policy(self):
        docs = render_chart(
            values={
                "otelCollector": {"tracesEnabled": True},
                "ipFamilyPolicy": "PreferDualStack",
                "ipFamilies": ["IPv4", "IPv6"],
            },
            show_only=[SERVICE_TEMPLATE],
        )

        assert jmespath.search("spec.ipFamilyPolicy", docs[0]) == "PreferDualStack"
        assert jmespath.search("spec.ipFamilies", docs[0]) == ["IPv4", "IPv6"]

        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True}},
            show_only=[SERVICE_TEMPLATE],
        )
        assert jmespath.search("spec.ipFamilies", docs[0]) is None


class TestOtelCollectorServiceAccount:
    def test_default_create(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True}}, show_only=[SERVICE_ACCOUNT_TEMPLATE]
        )
        assert docs[0]["kind"] == "ServiceAccount"
        assert docs[0]["automountServiceAccountToken"] is False

    def test_create_false_suppresses_sa(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True, "serviceAccount": {"create": False}}},
            show_only=[SERVICE_ACCOUNT_TEMPLATE],
        )
        assert docs == []

    def test_create_false_deployment_uses_default_sa(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True, "serviceAccount": {"create": False}}},
            show_only=[DEPLOYMENT_TEMPLATE],
        )
        assert jmespath.search("spec.template.spec.serviceAccountName", docs[0]) == "default"

    def test_custom_sa_name_propagates_to_deployment(self):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": True, "serviceAccount": {"name": "my-custom-sa"}}},
            show_only=[DEPLOYMENT_TEMPLATE, SERVICE_ACCOUNT_TEMPLATE],
        )
        sa = next(d for d in docs if d["kind"] == "ServiceAccount")
        deployment = next(d for d in docs if d["kind"] == "Deployment")
        assert sa["metadata"]["name"] == "my-custom-sa"
        assert jmespath.search("spec.template.spec.serviceAccountName", deployment) == "my-custom-sa"

    def test_automount_token_override(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "tracesEnabled": True,
                    "serviceAccount": {"automountServiceAccountToken": True},
                }
            },
            show_only=[SERVICE_ACCOUNT_TEMPLATE],
        )
        assert docs[0]["automountServiceAccountToken"] is True

    def test_sa_annotations(self):
        docs = render_chart(
            values={
                "otelCollector": {
                    "tracesEnabled": True,
                    "serviceAccount": {"annotations": {"sa_anno": "sa_value"}},
                }
            },
            show_only=[SERVICE_ACCOUNT_TEMPLATE],
        )
        assert jmespath.search("metadata.annotations.sa_anno", docs[0]) == "sa_value"


class TestOtelCollectorAirflowEnvironment:
    """The correct OTEL_* env vars can be found on Airflow pods based on the trace/metrics flags."""

    @pytest.mark.parametrize("template", AIRFLOW_POD_TEMPLATES)
    def test_default_emits_traces_env_only(self, template):
        docs = render_chart(values={"otelCollector": {"tracesEnabled": True}}, show_only=[template])
        names = jmespath.search("spec.template.spec.containers[0].env | [*].name", docs[0])

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
        names = jmespath.search("spec.template.spec.containers[0].env | [*].name", docs[0])

        assert "OTEL_SERVICE_NAME" in names
        assert "OTEL_EXPORTER_OTLP_PROTOCOL" in names
        assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" in names
        assert "OTEL_METRIC_EXPORT_INTERVAL" in names

        # Traces env vars aren't present.
        assert "OTEL_TRACES_EXPORTER" not in names
        assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in names

    @pytest.mark.parametrize("template", AIRFLOW_POD_TEMPLATES)
    def test_both_flags_off_emits_no_otel_env(self, template):
        docs = render_chart(
            values={"otelCollector": {"tracesEnabled": False, "metricsEnabled": False}},
            show_only=[template],
        )

        assert (
            jmespath.search("spec.template.spec.containers[0].env[?name.starts_with(@, 'OTEL_')]", docs[0])
            == []
        )

    def test_metric_export_interval_value(self):
        docs = render_chart(
            values={"otelCollector": {"metricsEnabled": True, "metricExportIntervalMs": 12345}},
            show_only=[AIRFLOW_POD_TEMPLATES[0]],
        )

        assert (
            jmespath.search(
                "spec.template.spec.containers[0].env[?name=='OTEL_METRIC_EXPORT_INTERVAL'] | [0].value",
                docs[0],
            )
            == "12345"
        )


class TestOtelCollectorAirflowConfig:
    """Renders the correct values in airflow.cfg."""

    @staticmethod
    def _get_conf_section(cfg: str, name: str) -> str:
        """Return the body of the specified section."""
        return cfg.split(f"[{name}]")[1].split("[")[0]

    @staticmethod
    def _get_airflow_conf(values=None):
        docs = render_chart(values=values or {}, show_only=[AIRFLOW_CONFIGMAP_TEMPLATE])
        return jmespath.search('data."airflow.cfg"', docs[0])

    def test_default_traces_section_has_otel_on_true(self):
        traces = self._get_conf_section(
            self._get_airflow_conf({"otelCollector": {"tracesEnabled": True}}), "traces"
        )
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
