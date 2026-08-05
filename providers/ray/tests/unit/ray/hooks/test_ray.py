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

import subprocess
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest
import yaml
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from ray.job_submission import JobStatus

from airflow.providers.ray.exceptions import RayAirflowException
from airflow.providers.ray.hooks.ray import RayHook


class TestRayHook:
    @pytest.fixture
    def ray_hook(self):
        with patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection") as mock_get_connection:
            mock_connection = Mock()
            mock_connection.extra_dejson = {
                "kube_config_path": None,
                "kube_config": None,
                "cluster_context": None,
            }
            mock_get_connection.return_value = mock_connection

            with patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__", return_value=None):
                hook = RayHook(conn_id="test_conn")
                # Manually set the necessary attributes
                hook.namespace = "default"
                hook.kubeconfig = "/path/to/kubeconfig"
                return hook

    @patch("airflow.providers.ray.hooks.ray.JobSubmissionClient")
    def test_ray_client(self, mock_job_client, ray_hook):
        mock_job_client.return_value = MagicMock()
        client = ray_hook.ray_client()
        assert isinstance(client, MagicMock)
        mock_job_client.assert_called_once_with(
            address=ray_hook.address,
            create_cluster_if_needed=ray_hook.create_cluster_if_needed,
            cookies=ray_hook.cookies,
            metadata=ray_hook.metadata,
            headers=ray_hook.headers,
            verify=ray_hook.verify,
        )

    @patch("airflow.providers.ray.hooks.ray.JobSubmissionClient")
    def test_submit_ray_job(self, mock_job_client, ray_hook):
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.submit_job.return_value = "test_job_id"
        job_id = ray_hook.submit_ray_job(dashboard_url="http://example.com", entrypoint="test_entry")
        assert job_id == "test_job_id"

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_setup_kubeconfig_path(self, mock_load_kube_config, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn")
        hook._setup_kubeconfig("/tmp/fake_kubeconfig", None, "test_context")

        assert hook.kubeconfig == "/tmp/fake_kubeconfig"
        mock_load_kube_config.assert_called_once_with(
            config_file="/tmp/fake_kubeconfig", context="test_context"
        )

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    @patch("tempfile.NamedTemporaryFile")
    def test_setup_kubeconfig_content(
        self, mock_tempfile, mock_load_kube_config, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        mock_tempfile.return_value.__enter__.return_value.name = "/tmp/fake_kubeconfig"
        mock_tempfile.return_value.__enter__.return_value.write = MagicMock()

        hook = RayHook(conn_id="test_conn")
        kubeconfig_content = "apiVersion: v1\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443"

        hook._setup_kubeconfig(None, kubeconfig_content, "test_context")

        mock_tempfile.return_value.__enter__.return_value.write.assert_called_once_with(
            kubeconfig_content.encode()
        )
        mock_load_kube_config.assert_called_once_with(
            config_file="/tmp/fake_kubeconfig", context="test_context"
        )

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    def test_setup_kubeconfig_invalid_config(self, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn")
        with pytest.raises(RayAirflowException) as exc_info:
            hook._setup_kubeconfig("/tmp/fake_kubeconfig", "kubeconfig_content", "test_context")

        assert str(exc_info.value) == (
            "Invalid connection configuration. Options kube_config_path and "
            "kube_config are mutually exclusive. You can only use one option at a time."
        )

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.JobSubmissionClient")
    def test_delete_ray_job(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.delete_job.return_value = "deleted"
        hook = RayHook(conn_id="test_conn")
        result = hook.delete_ray_job("http://example.com", job_id="test_job_id")
        assert result == "deleted"

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.JobSubmissionClient")
    def test_get_ray_job_status(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.get_job_status.return_value = JobStatus.SUCCEEDED
        hook = RayHook(conn_id="test_conn")
        status = hook.get_ray_job_status("http://example.com", "test_job_id")
        assert status == JobStatus.SUCCEEDED

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.JobSubmissionClient")
    def test_get_ray_job_logs(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.get_job_logs.return_value = "test logs"

        hook = RayHook(conn_id="test_conn")
        dashboard_url = "http://example.com:8265"
        job_id = "test_job_id"

        logs = hook.get_ray_job_logs(dashboard_url, job_id)

        assert logs == "test logs"
        mock_job_client.assert_called_once_with(
            address=dashboard_url,
            create_cluster_if_needed=False,
            cookies=None,
            metadata=None,
            headers={},
            verify=False,
        )
        mock_client_instance.get_job_logs.assert_called_once_with(job_id=job_id)

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.requests.get")
    @patch("builtins.open", new_callable=mock_open, read_data="key: value\n")
    def test_load_yaml_content(self, mock_open, mock_requests, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        hook = RayHook(conn_id="test_conn")
        result = hook.load_yaml_content("test_path")
        assert result == {"key": "value"}

        mock_requests.return_value.status_code = 200
        mock_requests.return_value.text = "key: value\n"
        result = hook.load_yaml_content("http://test-url")
        assert result == {"key": "value"}

    @patch("os.path.isfile")
    @patch("builtins.open", new_callable=mock_open, read_data="key: value\n")
    def test_validate_yaml_file_success(self, mock_file, mock_isfile, ray_hook):
        mock_isfile.return_value = True

        # Test with a valid YAML file
        ray_hook._validate_yaml_file("valid_file.yaml")

        mock_isfile.assert_called_once_with("valid_file.yaml")
        mock_file.assert_called_once_with("valid_file.yaml")

    @patch("os.path.isfile")
    @patch("builtins.open", new_callable=mock_open, read_data="invalid: yaml: content")
    def test_validate_yaml_file_invalid_yaml(self, mock_file, mock_isfile, ray_hook):
        mock_isfile.return_value = True

        # Test with an invalid YAML file
        with pytest.raises(RayAirflowException) as exc_info:
            with patch("yaml.safe_load", side_effect=yaml.YAMLError("Invalid YAML")):
                ray_hook._validate_yaml_file("invalid_file.yaml")

        assert "The specified YAML file is not valid YAML" in str(exc_info.value)
        mock_isfile.assert_called_once_with("invalid_file.yaml")
        mock_file.assert_called_once_with("invalid_file.yaml")

    @patch("os.path.isfile")
    def test_validate_yaml_file_not_exists(self, mock_isfile, ray_hook):
        mock_isfile.return_value = False

        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook._validate_yaml_file("non_existent_file.yaml")

        assert "The specified YAML file does not exist" in str(exc_info.value)
        mock_isfile.assert_called_once_with("non_existent_file.yaml")

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.socket.socket")
    def test_is_port_open(self, mock_socket, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_socket_instance = mock_socket.return_value

        # Test successful connection
        mock_socket_instance.connect.return_value = None
        hook = RayHook(conn_id="test_conn")
        result = hook._is_port_open("localhost", 8080)
        assert result is True

    @patch("airflow.providers.ray.hooks.ray.RayHook.core_v1_client")
    def test_get_service_success(self, mock_core_v1_client, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_core_v1_client.read_namespaced_service.return_value = mock_service

        service = ray_hook._get_service("test-service", "default")

        assert service == mock_service
        mock_core_v1_client.read_namespaced_service.assert_called_once_with("test-service", "default")

    @patch("airflow.providers.ray.hooks.ray.RayHook.core_v1_client")
    def test_get_service_not_found(self, mock_core_v1_client, ray_hook):
        mock_core_v1_client.read_namespaced_service.side_effect = client.exceptions.ApiException(status=404)

        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook._get_service("non-existent-service", "default")

        assert "Service non-existent-service not found" in str(exc_info.value)

    def test_get_load_balancer_details_with_ingress(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_ingress = Mock(spec=client.V1LoadBalancerIngress)
        mock_ingress.ip = "192.168.1.1"
        mock_ingress.hostname = None
        mock_service.status.load_balancer.ingress = [mock_ingress]

        mock_port = Mock()
        mock_port.name = "http"
        mock_port.port = 80
        mock_service.spec.ports = [mock_port]

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details == {"ip": "192.168.1.1", "hostname": None, "ports": [{"name": "http", "port": 80}]}

    def test_get_load_balancer_details_with_hostname(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_ingress = Mock(spec=client.V1LoadBalancerIngress)
        mock_ingress.ip = None
        mock_ingress.hostname = "example.com"
        mock_service.status.load_balancer.ingress = [mock_ingress]

        mock_port = Mock()
        mock_port.name = "https"
        mock_port.port = 443
        mock_service.spec.ports = [mock_port]

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details == {
            "hostname": "example.com",
            "ip": None,
            "ports": [{"name": "https", "port": 443}],
        }

    def test_get_load_balancer_details_no_ingress(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_service.status.load_balancer.ingress = None

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details is None

    def test_get_load_balancer_details_no_ip_or_hostname(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_ingress = Mock(spec=client.V1LoadBalancerIngress)
        mock_ingress.ip = None
        mock_ingress.hostname = None
        mock_service.status.load_balancer.ingress = [mock_ingress]

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details is None

    @patch("airflow.providers.ray.hooks.ray.RayHook.log")
    @patch("airflow.providers.ray.hooks.ray.subprocess.run")
    def test_run_command_exception(self, mock_subprocess_run, mock_log, ray_hook):
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="test command", output="test output", stderr="test error"
        )

        stdout, stderr = ray_hook._run_command(["test", "command"])

        assert stdout is None
        assert stderr is None

        mock_log.error.assert_any_call(
            "An error occurred while executing the command: %s", mock_subprocess_run.side_effect
        )
        mock_log.error.assert_any_call("Return code: %s", 1)
        mock_log.error.assert_any_call("Standard Output: %s", "test output")
        mock_log.error.assert_any_call("Standard Error: %s", "test error")

        mock_subprocess_run.assert_called_once_with(
            ["test", "command"],
            check=True,
            text=True,
            capture_output=True,
            env=ray_hook._run_command.__globals__["os"].environ.copy(),
        )

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.subprocess.run")
    def test_install_kuberay_operator(self, mock_subprocess_run, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_subprocess_run.return_value = MagicMock(stdout="install output", stderr="")

        hook = RayHook(conn_id="test_conn")
        stdout, stderr = hook.install_kuberay_operator(version="1.0.0")

        assert "install output" in stdout
        assert stderr == ""

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.subprocess.run")
    def test_uninstall_kuberay_operator(self, mock_subprocess_run, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_subprocess_run.return_value = MagicMock(stdout="uninstall output", stderr="")

        hook = RayHook(conn_id="test_conn")
        stdout, stderr = hook.uninstall_kuberay_operator()

        assert "uninstall output" in stdout
        assert stderr == ""

    @patch("airflow.providers.ray.hooks.ray.RayHook._get_service")
    @patch("airflow.providers.ray.hooks.ray.RayHook._get_load_balancer_details")
    @patch("airflow.providers.ray.hooks.ray.RayHook._check_load_balancer_readiness")
    def test_wait_for_load_balancer_success(
        self, mock_check_readiness, mock_get_lb_details, mock_get_service, ray_hook
    ):
        mock_service = Mock(spec=client.V1Service)
        mock_get_service.return_value = mock_service

        mock_get_lb_details.return_value = {
            "hostname": "test-lb.example.com",
            "ip": None,
            "ports": [{"name": "http", "port": 80}, {"name": "https", "port": 443}],
        }

        mock_check_readiness.return_value = "test-lb.example.com"

        result = ray_hook._wait_for_load_balancer(
            "test-service", namespace="default", max_retries=1, retry_interval=1
        )

        assert result == {
            "hostname": "test-lb.example.com",
            "ip": None,
            "ports": [{"name": "http", "port": 80}, {"name": "https", "port": 443}],
            "working_address": "test-lb.example.com",
        }
        mock_get_service.assert_called_once_with("test-service", "default")
        mock_get_lb_details.assert_called_once_with(mock_service)
        mock_check_readiness.assert_called_once()

    @patch("airflow.providers.ray.hooks.ray.RayHook._get_service")
    @patch("airflow.providers.ray.hooks.ray.RayHook._get_load_balancer_details")
    @patch("airflow.providers.ray.hooks.ray.RayHook._is_port_open")
    def test_wait_for_load_balancer_timeout(
        self, mock_is_port_open, mock_get_lb_details, mock_get_service, ray_hook
    ):
        mock_service = Mock(spec=client.V1Service)
        mock_get_service.return_value = mock_service

        mock_get_lb_details.return_value = {
            "hostname": "test-lb.example.com",
            "ip": None,
            "ports": [{"name": "http", "port": 80}],
        }

        mock_is_port_open.return_value = False

        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook._wait_for_load_balancer(
                "test-service", namespace="default", max_retries=2, retry_interval=1
            )

        assert "LoadBalancer did not become ready after 2 attempts" in str(exc_info.value)

    @patch("airflow.providers.ray.hooks.ray.RayHook._get_service")
    def test_wait_for_load_balancer_service_not_found(self, mock_get_service, ray_hook):
        mock_get_service.side_effect = RayAirflowException("Service test-service not found")

        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook._wait_for_load_balancer(
                "test-service", namespace="default", max_retries=1, retry_interval=1
            )

        assert "LoadBalancer did not become ready after 1 attempts" in str(exc_info.value)

    @patch("airflow.providers.ray.hooks.ray.RayHook._is_port_open")
    def test_check_load_balancer_readiness_ip(self, mock_is_port_open, ray_hook):
        mock_is_port_open.return_value = True
        lb_details = {"ip": "192.168.1.1", "hostname": None, "ports": [{"name": "http", "port": 80}]}

        result = ray_hook._check_load_balancer_readiness(lb_details)

        assert result == "192.168.1.1"
        mock_is_port_open.assert_called_once_with("192.168.1.1", 80)

    @patch("airflow.providers.ray.hooks.ray.RayHook._is_port_open")
    def test_check_load_balancer_readiness_hostname(self, mock_is_port_open, ray_hook):
        mock_is_port_open.side_effect = [False, True]
        lb_details = {
            "ip": "192.168.1.1",
            "hostname": "example.com",
            "ports": [{"name": "http", "port": 80}, {"name": "https", "port": 443}],
        }

        result = ray_hook._check_load_balancer_readiness(lb_details)

        assert result == "example.com"
        mock_is_port_open.assert_any_call("192.168.1.1", 80)
        mock_is_port_open.assert_any_call("example.com", 80)

    @patch("airflow.providers.ray.hooks.ray.RayHook._is_port_open")
    def test_check_load_balancer_readiness_not_ready(self, mock_is_port_open, ray_hook):
        mock_is_port_open.return_value = False
        lb_details = {"ip": "192.168.1.1", "hostname": "example.com", "ports": [{"name": "http", "port": 80}]}

        result = ray_hook._check_load_balancer_readiness(lb_details)

        assert result is None
        mock_is_port_open.assert_any_call("192.168.1.1", 80)
        mock_is_port_open.assert_any_call("example.com", 80)

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.read_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_get_daemon_set(
        self, mock_load_kube_config, mock_read_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        mock_metadata = MagicMock()
        mock_metadata.name = "test-daemonset"
        mock_read_daemon_set.return_value = MagicMock(metadata=mock_metadata)

        hook = RayHook(conn_id="test_conn")
        daemon_set = hook.get_daemon_set(name="test-daemonset")

        assert daemon_set.metadata.name == "test-daemonset"

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.read_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_get_daemon_set_not_found(
        self, mock_load_kube_config, mock_read_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_read_daemon_set.side_effect = ApiException(status=404, reason="Not Found")

        hook = RayHook(conn_id="test_conn")
        daemon_set = hook.get_daemon_set(name="test-daemonset")

        assert daemon_set is None

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.create_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_create_daemon_set(
        self, mock_load_kube_config, mock_create_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        mock_metadata = MagicMock()
        mock_metadata.name = "test-daemonset"
        mock_create_daemon_set.return_value = MagicMock(metadata=mock_metadata)

        hook = RayHook(conn_id="test_conn")
        body = {"metadata": {"name": "test-daemonset"}}
        daemon_set = hook.create_daemon_set(name="test-daemonset", body=body)

        assert daemon_set.metadata.name == "test-daemonset"

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.create_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_create_daemon_set_no_body(
        self, mock_load_kube_config, mock_create_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn")
        daemon_set = hook.create_daemon_set(name="test-daemonset", body=None)

        assert daemon_set is None

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.create_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_create_daemon_set_exception(
        self, mock_load_kube_config, mock_create_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_create_daemon_set.side_effect = ApiException(status=500, reason="Internal Server Error")

        hook = RayHook(conn_id="test_conn")
        body = {"metadata": {"name": "test-daemonset"}}
        daemon_set = hook.create_daemon_set(name="test-daemonset", body=body)

        assert daemon_set is None

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.delete_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_delete_daemon_set(
        self, mock_load_kube_config, mock_delete_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_delete_daemon_set.return_value = MagicMock(status="Success")

        hook = RayHook(conn_id="test_conn")
        response = hook.delete_daemon_set(name="test-daemonset")

        assert response.status == "Success"

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.delete_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_delete_daemon_set_not_found(
        self, mock_load_kube_config, mock_delete_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_delete_daemon_set.side_effect = ApiException(status=404, reason="Not Found")

        hook = RayHook(conn_id="test_conn")
        response = hook.delete_daemon_set(name="test-daemonset")

        assert response is None

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("airflow.providers.ray.hooks.ray.client.AppsV1Api.delete_namespaced_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.config.load_kube_config")
    def test_delete_daemon_set_exception(
        self, mock_load_kube_config, mock_delete_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_delete_daemon_set.side_effect = ApiException(status=500, reason="Internal Server Error")

        hook = RayHook(conn_id="test_conn")
        response = hook.delete_daemon_set(name="test-daemonset")

        assert response is None

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("os.path.isfile")
    def test_validate_yaml_file_not_found(self, mock_is_file, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_is_file.return_value = False

        hook = RayHook(conn_id="test_conn")
        with pytest.raises(RayAirflowException) as exc_info:
            hook._validate_yaml_file("test.yaml")

        assert "The specified YAML file does not exist" in str(exc_info.value)

    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.get_connection")
    @patch("airflow.providers.ray.hooks.ray.KubernetesHook.__init__")
    @patch("os.path.isfile")
    def test_validate_yaml_file_invalid_extension(
        self, mock_is_file, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_is_file.return_value = True

        hook = RayHook(conn_id="test_conn")
        with pytest.raises(RayAirflowException) as exc_info:
            hook._validate_yaml_file("test.txt")

        assert "The specified YAML file must have a .yaml or .yml extension" in str(exc_info.value)

    @patch("airflow.providers.ray.hooks.ray.RayHook._validate_yaml_file")
    @patch("airflow.providers.ray.hooks.ray.RayHook.install_kuberay_operator")
    @patch("airflow.providers.ray.hooks.ray.RayHook.load_yaml_content")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook.create_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook._setup_gpu_driver")
    @patch("airflow.providers.ray.hooks.ray.RayHook._setup_load_balancer")
    def test_setup_ray_cluster_success(
        self,
        mock_setup_load_balancer,
        mock_setup_gpu_driver,
        mock_create_custom_object,
        mock_get_custom_object,
        mock_load_yaml_content,
        mock_install_kuberay_operator,
        mock_validate_yaml_file,
        ray_hook,
    ):
        mock_load_yaml_content.return_value = {
            "kind": "RayCluster",
            "apiVersion": "ray.io/v1",
            "metadata": {"name": "test-cluster"},
        }
        mock_get_custom_object.side_effect = ApiException(status=404)

        context = {"task_instance": MagicMock()}
        ray_hook.setup_ray_cluster(
            context=context,
            ray_cluster_yaml="test.yaml",
            kuberay_version="1.0.0",
            gpu_device_plugin_yaml="gpu.yaml",
            update_if_exists=False,
        )

        mock_validate_yaml_file.assert_called_once_with("test.yaml")
        mock_install_kuberay_operator.assert_called_once_with(version="1.0.0")
        mock_load_yaml_content.assert_called_once_with("test.yaml")
        mock_create_custom_object.assert_called_once()
        mock_setup_gpu_driver.assert_called_once_with(gpu_device_plugin_yaml="gpu.yaml")
        mock_setup_load_balancer.assert_called_once()

    @patch("airflow.providers.ray.hooks.ray.RayHook._validate_yaml_file")
    @patch("airflow.providers.ray.hooks.ray.RayHook.uninstall_kuberay_operator")
    @patch("airflow.providers.ray.hooks.ray.RayHook.load_yaml_content")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook.delete_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.RayHook.delete_daemon_set")
    def test_delete_ray_cluster_success(
        self,
        mock_delete_daemon_set,
        mock_get_daemon_set,
        mock_delete_custom_object,
        mock_get_custom_object,
        mock_load_yaml_content,
        mock_uninstall_kuberay_operator,
        mock_validate_yaml_file,
        ray_hook,
    ):
        mock_load_yaml_content.return_value = {
            "kind": "RayCluster",
            "apiVersion": "ray.io/v1",
            "metadata": {"name": "test-cluster"},
        }
        mock_get_daemon_set.return_value = MagicMock()
        mock_get_custom_object.return_value = MagicMock()

        ray_hook.delete_ray_cluster(
            ray_cluster_yaml="test.yaml",
            gpu_device_plugin_yaml="gpu.yaml",
        )

        mock_validate_yaml_file.assert_called_once_with("test.yaml")
        mock_load_yaml_content.assert_called_with("test.yaml")
        mock_get_daemon_set.assert_called_once()
        mock_delete_daemon_set.assert_called_once()
        mock_delete_custom_object.assert_called_once()
        mock_uninstall_kuberay_operator.assert_called_once()

    @patch("airflow.providers.ray.hooks.ray.RayHook._validate_yaml_file")
    @patch("airflow.providers.ray.hooks.ray.RayHook._delete_ray_cluster_crd")
    @patch("airflow.providers.ray.hooks.ray.RayHook.uninstall_kuberay_operator")
    def test_delete_ray_cluster_without_gpu_plugin(
        self, mock_uninstall_kuberay_operator, mock_delete_ray_cluster_crd, mock_validate_yaml_file, ray_hook
    ):
        ray_hook.delete_ray_cluster(ray_cluster_yaml="test.yaml", gpu_device_plugin_yaml="")

        mock_validate_yaml_file.assert_called_once_with("test.yaml")
        mock_delete_ray_cluster_crd.assert_called_once_with(ray_cluster_yaml="test.yaml")
        mock_uninstall_kuberay_operator.assert_called_once_with()

    @patch("airflow.providers.ray.hooks.ray.JobSubmissionClient")
    def test_ray_client_exception(self, mock_job_client, ray_hook):
        mock_job_client.side_effect = Exception("Connection failed")
        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook.ray_client()
        assert str(exc_info.value) == "Failed to create Ray JobSubmissionClient: Connection failed"

    @patch("airflow.providers.ray.hooks.ray.RayHook.get_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook.create_custom_object")
    def test_create_or_update_cluster_exception(self, mock_create, mock_get, ray_hook):
        mock_get.side_effect = client.exceptions.ApiException(status=500, reason="Internal Server Error")
        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook._create_or_update_cluster(
                update_if_exists=False,
                group="ray.io",
                version="v1",
                plural="rayclusters",
                name="test-cluster",
                namespace="default",
                cluster_spec={},
            )
        assert "Error accessing Ray cluster 'test-cluster'" in str(exc_info.value)

    @patch("airflow.providers.ray.hooks.ray.RayHook.get_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook.custom_object_client")
    def test_create_or_update_cluster_update(self, mock_client, mock_get, ray_hook):
        mock_get.return_value = {"metadata": {"name": "test-cluster"}}
        ray_hook._create_or_update_cluster(
            update_if_exists=True,
            group="ray.io",
            version="v1",
            plural="rayclusters",
            name="test-cluster",
            namespace="default",
            cluster_spec={"spec": {"some": "config"}},
        )
        mock_client.patch_namespaced_custom_object.assert_called_once_with(
            group="ray.io",
            version="v1",
            namespace="default",
            plural="rayclusters",
            name="test-cluster",
            body={"spec": {"some": "config"}},
        )

    @patch("airflow.providers.ray.hooks.ray.RayHook._validate_yaml_file")
    @patch("airflow.providers.ray.hooks.ray.RayHook.install_kuberay_operator")
    @patch("airflow.providers.ray.hooks.ray.RayHook.load_yaml_content")
    @patch("airflow.providers.ray.hooks.ray.RayHook._create_or_update_cluster")
    @patch("airflow.providers.ray.hooks.ray.RayHook._setup_gpu_driver")
    @patch("airflow.providers.ray.hooks.ray.RayHook._setup_load_balancer")
    def test_setup_ray_cluster_exception(
        self,
        mock_setup_lb,
        mock_setup_gpu,
        mock_create_or_update,
        mock_load_yaml,
        mock_install_operator,
        mock_validate_yaml,
        ray_hook,
    ):
        mock_create_or_update.side_effect = Exception("Cluster creation failed")
        context = {"task_instance": MagicMock()}
        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook.setup_ray_cluster(
                context=context,
                ray_cluster_yaml="test.yaml",
                kuberay_version="1.0.0",
                gpu_device_plugin_yaml="gpu.yaml",
                update_if_exists=False,
            )
        assert "Failed to set up Ray cluster: Cluster creation failed" in str(exc_info.value)

    @patch("airflow.providers.ray.hooks.ray.RayHook._validate_yaml_file")
    @patch("airflow.providers.ray.hooks.ray.RayHook.load_yaml_content")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook.delete_custom_object")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.RayHook.delete_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.RayHook.uninstall_kuberay_operator")
    def test_delete_ray_cluster_exception(
        self,
        mock_uninstall_operator,
        mock_delete_daemon_set,
        mock_get_daemon_set,
        mock_delete_custom_object,
        mock_get_custom_object,
        mock_load_yaml,
        mock_validate_yaml,
        ray_hook,
    ):
        mock_delete_custom_object.side_effect = Exception("Cluster deletion failed")
        with pytest.raises(RayAirflowException) as exc_info:
            ray_hook.delete_ray_cluster(ray_cluster_yaml="test.yaml", gpu_device_plugin_yaml="gpu.yaml")
        assert "Failed to delete Ray cluster: Cluster deletion failed" in str(exc_info.value)

    @patch("airflow.providers.ray.hooks.ray.RayHook.create_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_daemon_set", return_value=True)
    def test_setup_ray_cluster_with_config_existing_daemon(
        self, mock_get_daemon_set, mock_create_daemon_set, ray_hook
    ):
        gpu_device_plugin_yaml = (
            "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml"
        )
        response = ray_hook._setup_gpu_driver(gpu_device_plugin_yaml)
        assert response is None
        mock_get_daemon_set.assert_called_once()
        mock_create_daemon_set.assert_not_called()

    @patch("airflow.providers.ray.hooks.ray.RayHook.create_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_daemon_set", return_value=False)
    def test_setup_ray_cluster_with_config_inexistent_daemon(
        self, mock_get_daemon_set, mock_create_daemon_set, ray_hook
    ):
        gpu_device_plugin_yaml = (
            "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml"
        )
        response = ray_hook._setup_gpu_driver(gpu_device_plugin_yaml)
        assert response is None
        mock_get_daemon_set.assert_called_once()
        mock_create_daemon_set.assert_called_once()

    @patch("airflow.providers.ray.hooks.ray.RayHook.create_daemon_set")
    @patch("airflow.providers.ray.hooks.ray.RayHook.get_daemon_set")
    def test_setup_ray_cluster_no_config(self, mock_get_daemon_set, mock_create_daemon_set, ray_hook):
        gpu_device_plugin_yaml = ""
        response = ray_hook._setup_gpu_driver(gpu_device_plugin_yaml)
        assert response is None
        mock_get_daemon_set.assert_not_called()
        mock_create_daemon_set.assert_not_called()
