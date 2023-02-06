import jmespath
import pytest

from tests.charts.helm_template_generator import render_chart


class LogGroomerTestBase:
    obj_name: str = ""

    def test_log_groomer_collector_can_be_disabled(self):
        docs = render_chart(
            values={f"{self.obj_name}": {"logGroomerSidecar": {"enabled": False}}},
            show_only=[f"templates/{self.obj_name}/{self.obj_name}-deployment.yaml"],
        )
        assert 1 == len(jmespath.search("spec.template.spec.containers", docs[0]))

    def test_log_groomer_collector_default_command_and_args(self):
        docs = render_chart(show_only=[f"templates/{self.obj_name}/{self.obj_name}-deployment.yaml"])

        assert jmespath.search("spec.template.spec.containers[1].command", docs[0]) is None
        assert ["bash", "/clean-logs"] == jmespath.search("spec.template.spec.containers[1].args", docs[0])

    def test_log_groomer_collector_default_retention_days(self):
        docs = render_chart(show_only=[f"templates/{self.obj_name}/{self.obj_name}-deployment.yaml"])

        assert "AIRFLOW__LOG_RETENTION_DAYS" == jmespath.search(
            "spec.template.spec.containers[1].env[0].name", docs[0]
        )
        assert "15" == jmespath.search("spec.template.spec.containers[1].env[0].value", docs[0])

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_log_groomer_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={f"{self.obj_name}": {"logGroomerSidecar": {"command": command, "args": args}}},
            show_only=[f"templates/{self.obj_name}/{self.obj_name}-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[1].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[1].args", docs[0])

    def test_log_groomer_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                f"{self.obj_name}": {
                    "logGroomerSidecar": {
                        "command": ["{{ .Release.Name }}"],
                        "args": ["{{ .Release.Service }}"],
                    }
                }
            },
            show_only=[f"templates/{self.obj_name}/{self.obj_name}-deployment.yaml"],
        )

        assert ["release-name"] == jmespath.search("spec.template.spec.containers[1].command", docs[0])
        assert ["Helm"] == jmespath.search("spec.template.spec.containers[1].args", docs[0])

    @pytest.mark.parametrize(
        "retention_days, retention_result",
        [
            (None, None),
            (30, "30"),
        ],
    )
    def test_log_groomer_retention_days_overrides(self, retention_days, retention_result):
        docs = render_chart(
            values={f"{self.obj_name}": {"logGroomerSidecar": {"retentionDays": retention_days}}},
            show_only=[f"templates/{self.obj_name}/{self.obj_name}-deployment.yaml"],
        )

        if retention_result:
            assert "AIRFLOW__LOG_RETENTION_DAYS" == jmespath.search(
                "spec.template.spec.containers[1].env[0].name", docs[0]
            )
            assert retention_result == jmespath.search(
                "spec.template.spec.containers[1].env[0].value", docs[0]
            )
        else:
            assert jmespath.search("spec.template.spec.containers[1].env", docs[0]) is None

    def test_log_groomer_resources(self):
        docs = render_chart(
            values={
                f"{self.obj_name}": {
                    "logGroomerSidecar": {
                        "resources": {
                            "requests": {"memory": "2Gi", "cpu": "1"},
                            "limits": {"memory": "3Gi", "cpu": "2"},
                        }
                    }
                }
            },
            show_only=[f"templates/{self.obj_name}/{self.obj_name}-deployment.yaml"],
        )

        assert {
            "limits": {
                "cpu": "2",
                "memory": "3Gi",
            },
            "requests": {
                "cpu": "1",
                "memory": "2Gi",
            },
        } == jmespath.search("spec.template.spec.containers[1].resources", docs[0])
