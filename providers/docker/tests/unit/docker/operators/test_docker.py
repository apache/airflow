#
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

import logging
from unittest import mock
from unittest.mock import call

import pytest
from docker import APIClient
from docker.errors import APIError
from docker.types import Mount, Ulimit

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.docker.exceptions import DockerContainerFailedException
from airflow.providers.docker.operators.docker import DockerOperator, fetch_logs

TEST_CONN_ID = "docker_test_connection"
TEST_DOCKER_URL = "unix://var/run/docker.test.sock"
TEST_API_VERSION = "1.19"  # Keep it as low version might prevent call non-mocked docker api
TEST_IMAGE = "apache/airflow:latest"
TEST_CONTAINER_HOSTNAME = "test.container.host"
TEST_HOST_TEMP_DIRECTORY = "/tmp/host/dir"
TEST_AIRFLOW_TEMP_DIRECTORY = "/tmp/airflow/dir"
TEST_ENTRYPOINT = '["sh", "-c"]'

TEMPDIR_MOCK_RETURN_VALUE = "/mkdtemp"


@pytest.mark.parametrize("docker_conn_id", [pytest.param(None, id="empty-conn-id"), TEST_CONN_ID])
@pytest.mark.parametrize(
    "tls_params",
    [
        pytest.param({}, id="empty-tls-params"),
        pytest.param(
            {
                "tls_ca_cert": "foo",
                "tls_client_cert": "bar",
                "tls_client_key": "spam",
                "tls_verify": True,
                "tls_hostname": "egg",
                "tls_ssl_version": "super-secure",
            },
            id="all-tls-params",
        ),
    ],
)
def test_hook_usage(docker_hook_patcher, docker_conn_id, tls_params: dict):
    """Test that operator use DockerHook."""
    docker_hook_patcher.construct_tls_config.return_value = "MOCK-TLS-VALUE"
    expected_tls_call_args = {
        "ca_cert": tls_params.get("tls_ca_cert"),
        "client_cert": tls_params.get("tls_client_cert"),
        "client_key": tls_params.get("tls_client_key"),
        "verify": tls_params.get("tls_verify", True),
        "assert_hostname": tls_params.get("tls_hostname"),
        "ssl_version": tls_params.get("tls_ssl_version"),
    }

    op = DockerOperator(
        task_id="test_hook_usage_without_tls",
        api_version=TEST_API_VERSION,
        docker_conn_id=docker_conn_id,
        image=TEST_IMAGE,
        docker_url=TEST_DOCKER_URL,
        timeout=42,
        **tls_params,
    )
    hook = op.hook

    docker_hook_patcher.assert_called_once_with(
        docker_conn_id=docker_conn_id,
        base_url=TEST_DOCKER_URL,
        version=TEST_API_VERSION,
        tls="MOCK-TLS-VALUE",
        timeout=42,
    )
    docker_hook_patcher.construct_tls_config.assert_called_once_with(**expected_tls_call_args)

    # Check that ``DockerOperator.cli`` property return the same object as ``hook.api_client``.
    assert op.cli is hook.api_client


@pytest.mark.parametrize(
    "env_str, expected",
    [
        pytest.param("FOO=BAR\nSPAM=EGG", {"FOO": "BAR", "SPAM": "EGG"}, id="parsable-string"),
        pytest.param("", {}, id="empty-string"),
    ],
)
def test_unpack_environment_variables(env_str, expected):
    assert DockerOperator.unpack_environment_variables(env_str) == expected


@pytest.mark.parametrize("container_exists", [True, False])
def test_on_kill_client_created(docker_api_client_patcher, container_exists):
    """Test operator on_kill method if APIClient created."""
    op = DockerOperator(image=TEST_IMAGE, hostname=TEST_DOCKER_URL, task_id="test_on_kill")
    op.container = {"Id": "some_id"} if container_exists else None

    op.hook.get_conn()  # Try to create APIClient
    op.on_kill()
    if container_exists:
        docker_api_client_patcher.return_value.stop.assert_called_once_with("some_id")
    else:
        docker_api_client_patcher.return_value.stop.assert_not_called()


def test_on_kill_client_not_created(docker_api_client_patcher):
    """Test operator on_kill method if APIClient not created in case of error."""
    docker_api_client_patcher.side_effect = APIError("Fake Client Error")
    mock_container = mock.MagicMock()

    op = DockerOperator(image=TEST_IMAGE, hostname=TEST_DOCKER_URL, task_id="test_on_kill")
    op.container = mock_container

    with pytest.raises(APIError, match="Fake Client Error"):
        op.hook.get_conn()
    op.on_kill()
    docker_api_client_patcher.return_value.stop.assert_not_called()
    mock_container.assert_not_called()


class TestDockerOperator:
    @pytest.fixture(autouse=True)
    def setup_patchers(self, docker_api_client_patcher):
        self.tempdir_patcher = mock.patch("airflow.providers.docker.operators.docker.TemporaryDirectory")
        self.tempdir_mock = self.tempdir_patcher.start()
        self.tempdir_mock.return_value.__enter__.return_value = TEMPDIR_MOCK_RETURN_VALUE

        self.client_mock = mock.Mock(spec=APIClient)
        self.client_mock.create_container.return_value = {"Id": "some_id"}
        self.client_mock.images.return_value = []
        self.client_mock.pull.return_value = {"status": "pull log"}
        self.client_mock.wait.return_value = {"StatusCode": 0}
        self.client_mock.create_host_config.return_value = mock.Mock()
        self.log_messages = ["container log  😁   ", b"byte string container log"]
        self.client_mock.attach.return_value = self.log_messages

        # If logs() is called with tail then only return the last value, otherwise return the whole log.
        self.client_mock.logs.side_effect = (
            lambda **kwargs: iter(self.log_messages[-kwargs["tail"] :])
            if "tail" in kwargs
            else iter(self.log_messages)
        )

        docker_api_client_patcher.return_value = self.client_mock

        def dotenv_mock_return_value(**kwargs):
            env_dict = {}
            env_str = kwargs["stream"]
            for env_var in env_str.splitlines():
                key, _, val = env_var.partition("=")
                env_dict[key] = val
            return env_dict

        self.dotenv_patcher = mock.patch("airflow.providers.docker.operators.docker.dotenv_values")
        self.dotenv_mock = self.dotenv_patcher.start()
        self.dotenv_mock.side_effect = dotenv_mock_return_value

        yield

        self.tempdir_patcher.stop()
        self.dotenv_patcher.stop()

    def test_private_environment_is_private(self):
        operator = DockerOperator(
            private_environment={"PRIVATE": "MESSAGE"}, image=TEST_IMAGE, task_id="unittest"
        )
        assert operator._private_environment == {"PRIVATE": "MESSAGE"}, (
            "To keep this private, it must be an underscored attribute."
        )

    def test_execute_unicode_logs(self):
        self.client_mock.attach.return_value = ["unicode container log 😁"]

        original_raise_exceptions = logging.raiseExceptions
        logging.raiseExceptions = True

        operator = DockerOperator(image=TEST_IMAGE, owner="unittest", task_id="unittest")

        with mock.patch("traceback.print_exception") as print_exception_mock:
            operator.execute(None)
            logging.raiseExceptions = original_raise_exceptions
            print_exception_mock.assert_not_called()

    @pytest.mark.parametrize(
        "kwargs, actual_exit_code, expected_exc",
        [
            ({}, 0, None),
            ({}, 100, AirflowException),
            ({}, 101, AirflowException),
            ({"skip_on_exit_code": None}, 0, None),
            ({"skip_on_exit_code": None}, 100, AirflowException),
            ({"skip_on_exit_code": None}, 101, AirflowException),
            ({"skip_on_exit_code": 100}, 0, None),
            ({"skip_on_exit_code": 100}, 100, AirflowSkipException),
            ({"skip_on_exit_code": 100}, 101, AirflowException),
            ({"skip_on_exit_code": 0}, 0, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 0, None),
            ({"skip_on_exit_code": [100]}, 100, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 101, AirflowException),
            ({"skip_on_exit_code": [100, 102]}, 101, AirflowException),
            ({"skip_on_exit_code": (100,)}, 0, None),
            ({"skip_on_exit_code": (100,)}, 100, AirflowSkipException),
            ({"skip_on_exit_code": (100,)}, 101, AirflowException),
        ],
    )
    def test_skip(self, kwargs, actual_exit_code, expected_exc):
        msg = {"StatusCode": actual_exit_code}
        self.client_mock.wait.return_value = msg

        operator = DockerOperator(image="ubuntu", owner="unittest", task_id="unittest", **kwargs)

        if expected_exc is None:
            operator.execute({})
        else:
            with pytest.raises(expected_exc):
                operator.execute({})

    def test_execute_container_fails(self):
        failed_msg = {"StatusCode": 1}
        log_line = ["unicode container log 😁   ", b"byte string container log"]
        expected_message = "Docker container failed: {failed_msg}"
        self.client_mock.attach.return_value = log_line
        self.client_mock.wait.return_value = failed_msg

        operator = DockerOperator(image="ubuntu", owner="unittest", task_id="unittest")

        with pytest.raises(DockerContainerFailedException) as raised_exception:
            operator.execute(None)

        assert str(raised_exception.value) == expected_message.format(
            failed_msg=failed_msg,
        )
        assert raised_exception.value.logs == [log_line[0].strip(), log_line[1].decode("utf-8")]

    def test_auto_remove_container_fails(self):
        self.client_mock.wait.return_value = {"StatusCode": 1}
        operator = DockerOperator(image="ubuntu", owner="unittest", task_id="unittest", auto_remove="success")
        operator.container = {"Id": "some_id"}
        with pytest.raises(AirflowException):
            operator.execute(None)

        self.client_mock.remove_container.assert_called_once_with("some_id")

    def test_execute_xcom_behavior(self):
        self.client_mock.pull.return_value = [b'{"status":"pull log"}']
        kwargs = {
            "api_version": "1.19",
            "command": "env",
            "environment": {"UNIT": "TEST"},
            "private_environment": {"PRIVATE": "MESSAGE"},
            "image": "ubuntu:latest",
            "network_mode": "bridge",
            "owner": "unittest",
            "task_id": "unittest",
            "mounts": [Mount(source="/host/path", target="/container/path", type="bind")],
            "working_dir": "/container/path",
            "shm_size": 1000,
            "host_tmp_dir": "/host/airflow",
            "container_name": "test_container",
            "tty": True,
        }

        xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=False)
        xcom_all_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=True)
        no_xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=False)

        xcom_push_result = xcom_push_operator.execute(None)
        xcom_all_result = xcom_all_operator.execute(None)
        no_xcom_push_result = no_xcom_push_operator.execute(None)

        assert xcom_push_result == "byte string container log"
        assert xcom_all_result == ["container log  😁", "byte string container log"]
        assert no_xcom_push_result is None

    def test_execute_xcom_behavior_bytes(self):
        self.log_messages = [b"container log 1 ", b"container log 2"]
        self.client_mock.pull.return_value = [b'{"status":"pull log"}']
        self.client_mock.attach.return_value = iter([b"container log 1 ", b"container log 2"])
        # Make sure the logs side effect is updated after the change
        self.client_mock.attach.side_effect = (
            lambda **kwargs: iter(self.log_messages[-kwargs["tail"] :])
            if "tail" in kwargs
            else iter(self.log_messages)
        )

        kwargs = {
            "api_version": "1.19",
            "command": "env",
            "environment": {"UNIT": "TEST"},
            "private_environment": {"PRIVATE": "MESSAGE"},
            "image": "ubuntu:latest",
            "network_mode": "bridge",
            "owner": "unittest",
            "task_id": "unittest",
            "mounts": [Mount(source="/host/path", target="/container/path", type="bind")],
            "working_dir": "/container/path",
            "shm_size": 1000,
            "host_tmp_dir": "/host/airflow",
            "container_name": "test_container",
            "tty": True,
        }

        xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=False)
        xcom_all_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=True)
        no_xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=False)

        xcom_push_result = xcom_push_operator.execute(None)
        xcom_all_result = xcom_all_operator.execute(None)
        no_xcom_push_result = no_xcom_push_operator.execute(None)

        # Those values here are different than log above as they are from setup
        assert xcom_push_result == "container log 2"
        assert xcom_all_result == ["container log 1", "container log 2"]
        assert no_xcom_push_result is None

    def test_execute_xcom_behavior_no_result(self):
        self.log_messages = []
        self.client_mock.pull.return_value = [b'{"status":"pull log"}']
        self.client_mock.attach.return_value = iter([])

        kwargs = {
            "api_version": "1.19",
            "command": "env",
            "environment": {"UNIT": "TEST"},
            "private_environment": {"PRIVATE": "MESSAGE"},
            "image": "ubuntu:latest",
            "network_mode": "bridge",
            "owner": "unittest",
            "task_id": "unittest",
            "mounts": [Mount(source="/host/path", target="/container/path", type="bind")],
            "working_dir": "/container/path",
            "shm_size": 1000,
            "host_tmp_dir": "/host/airflow",
            "container_name": "test_container",
            "tty": True,
        }

        xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=False)
        xcom_all_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=True)
        no_xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=False)

        xcom_push_result = xcom_push_operator.execute(None)
        xcom_all_result = xcom_all_operator.execute(None)
        no_xcom_push_result = no_xcom_push_operator.execute(None)

        assert xcom_push_result is None
        assert xcom_all_result is None
        assert no_xcom_push_result is None

    def test_extra_hosts(self):
        hosts_obj = mock.Mock()
        operator = DockerOperator(task_id="test", image="test", extra_hosts=hosts_obj)
        operator.execute(None)
        self.client_mock.create_container.assert_called_once()
        assert "host_config" in self.client_mock.create_container.call_args.kwargs
        assert "extra_hosts" in self.client_mock.create_host_config.call_args.kwargs
        assert hosts_obj is self.client_mock.create_host_config.call_args.kwargs["extra_hosts"]

    def test_privileged(self):
        privileged = mock.Mock()
        operator = DockerOperator(task_id="test", image="test", privileged=privileged)
        operator.execute(None)
        self.client_mock.create_container.assert_called_once()
        assert "host_config" in self.client_mock.create_container.call_args.kwargs
        assert "privileged" in self.client_mock.create_host_config.call_args.kwargs
        assert privileged is self.client_mock.create_host_config.call_args.kwargs["privileged"]

    def test_port_bindings(self):
        port_bindings = {8000: 8080}
        operator = DockerOperator(task_id="test", image="test", port_bindings=port_bindings)
        operator.execute(None)
        self.client_mock.create_container.assert_called_once()
        assert "host_config" in self.client_mock.create_container.call_args.kwargs
        assert "port_bindings" in self.client_mock.create_host_config.call_args.kwargs
        assert port_bindings == self.client_mock.create_host_config.call_args.kwargs["port_bindings"]

    def test_ulimits(self):
        ulimits = [Ulimit(name="nofile", soft=1024, hard=2048)]
        operator = DockerOperator(task_id="test", image="test", ulimits=ulimits)
        operator.execute(None)
        self.client_mock.create_container.assert_called_once()
        assert "host_config" in self.client_mock.create_container.call_args.kwargs
        assert "ulimits" in self.client_mock.create_host_config.call_args.kwargs
        assert ulimits == self.client_mock.create_host_config.call_args.kwargs["ulimits"]

    @pytest.mark.parametrize(
        "auto_remove",
        ["True", "false", pytest.param(None, id="none"), pytest.param(None, id="empty"), "here-and-now"],
    )
    def test_auto_remove_invalid(self, auto_remove):
        with pytest.raises(ValueError, match="Invalid `auto_remove` value"):
            DockerOperator(task_id="test", image="test", auto_remove=auto_remove)

    def test_respect_docker_host_env(self, monkeypatch):
        monkeypatch.setenv("DOCKER_HOST", "tcp://docker-host-from-env:2375")
        operator = DockerOperator(task_id="test", image="test")
        assert operator.docker_url == "tcp://docker-host-from-env:2375"

    def test_docker_host_env_empty(self, monkeypatch):
        monkeypatch.setenv("DOCKER_HOST", "")
        operator = DockerOperator(task_id="test", image="test")
        # The docker CLI ignores the empty string and defaults to unix://var/run/docker.sock
        # We want to ensure the same behavior.
        assert operator.docker_url == "unix://var/run/docker.sock"

    def test_docker_host_env_unset(self, monkeypatch):
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        operator = DockerOperator(task_id="test", image="test")
        assert operator.docker_url == "unix://var/run/docker.sock"

    @pytest.mark.parametrize(
        "log_lines, expected_lines",
        [
            pytest.param(
                [
                    "return self.main(*args, **kwargs)",
                    "                 ^^^^^^^^^^^^^^^^",
                ],
                [
                    "return self.main(*args, **kwargs)",
                    "                 ^^^^^^^^^^^^^^^^",
                ],
                id="should-not-remove-leading-spaces",
            ),
            pytest.param(
                [
                    "   ^^^^^^^^^^^^^^^^   ",
                ],
                [
                    "   ^^^^^^^^^^^^^^^^",
                ],
                id="should-remove-trailing-spaces",
            ),
        ],
    )
    @mock.patch("logging.Logger")
    def test_fetch_logs(self, logger_mock, log_lines, expected_lines):
        fetch_logs(log_lines, logger_mock)
        assert logger_mock.info.call_args_list == [call("%s", line) for line in expected_lines]

    @pytest.mark.parametrize("labels", ({"key": "value"}, ["key=value"]))
    def test_labels(self, labels: dict[str, str] | list[str]):
        operator = DockerOperator(task_id="test", image="test", labels=labels)
        operator.execute({})
        self.client_mock.create_container.assert_called_once()
        assert "labels" in self.client_mock.create_container.call_args.kwargs
        assert labels == self.client_mock.create_container.call_args.kwargs["labels"]
