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
from docker.constants import DEFAULT_TIMEOUT_SECONDS
from docker.errors import APIError
from docker.types import DeviceRequest, LogConfig, Mount

from airflow.exceptions import AirflowException
from airflow.providers.docker.hooks.docker import DockerHook
from airflow.providers.docker.operators.docker import DockerOperator

TEMPDIR_MOCK_RETURN_VALUE = "/mkdtemp"


class TestDockerOperator:
    def setup_method(self):
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

        self.client_class_patcher = mock.patch(
            "airflow.providers.docker.operators.docker.APIClient",
            return_value=self.client_mock,
        )
        self.client_class_mock = self.client_class_patcher.start()

        def dotenv_mock_return_value(**kwargs):
            env_dict = {}
            env_str = kwargs["stream"]
            for env_var in env_str.split("\n"):
                kv = env_var.split("=")
                env_dict[kv[0]] = kv[1]
            return env_dict

        self.dotenv_patcher = mock.patch("airflow.providers.docker.operators.docker.dotenv_values")
        self.dotenv_mock = self.dotenv_patcher.start()
        self.dotenv_mock.side_effect = dotenv_mock_return_value

    def teardown_method(self) -> None:
        self.tempdir_patcher.stop()
        self.client_class_patcher.stop()
        self.dotenv_patcher.stop()

    def test_execute(self):
        stringio_patcher = mock.patch("airflow.providers.docker.operators.docker.StringIO")
        stringio_mock = stringio_patcher.start()
        stringio_mock.side_effect = lambda *args: args[0]

        operator = DockerOperator(
            api_version="1.19",
            command="env",
            environment={"UNIT": "TEST"},
            private_environment={"PRIVATE": "MESSAGE"},
            env_file="ENV=FILE\nVAR=VALUE",
            image="ubuntu:latest",
            network_mode="bridge",
            owner="unittest",
            task_id="unittest",
            mounts=[Mount(source="/host/path", target="/container/path", type="bind")],
            entrypoint='["sh", "-c"]',
            working_dir="/container/path",
            shm_size=1000,
            host_tmp_dir="/host/airflow",
            container_name="test_container",
            tty=True,
            device_requests=[DeviceRequest(count=-1, capabilities=[["gpu"]])],
            log_opts_max_file="5",
            log_opts_max_size="10m",
        )
        operator.execute(None)

        self.client_class_mock.assert_called_once_with(
            base_url="unix://var/run/docker.sock", tls=None, version="1.19", timeout=DEFAULT_TIMEOUT_SECONDS
        )

        self.client_mock.create_container.assert_called_once_with(
            command="env",
            name="test_container",
            environment={
                "AIRFLOW_TMP_DIR": "/tmp/airflow",
                "UNIT": "TEST",
                "PRIVATE": "MESSAGE",
                "ENV": "FILE",
                "VAR": "VALUE",
            },
            host_config=self.client_mock.create_host_config.return_value,
            image="ubuntu:latest",
            user=None,
            entrypoint=["sh", "-c"],
            working_dir="/container/path",
            tty=True,
        )
        self.client_mock.create_host_config.assert_called_once_with(
            mounts=[
                Mount(source="/host/path", target="/container/path", type="bind"),
                Mount(source="/mkdtemp", target="/tmp/airflow", type="bind"),
            ],
            network_mode="bridge",
            shm_size=1000,
            cpu_shares=1024,
            mem_limit=None,
            auto_remove=False,
            dns=None,
            dns_search=None,
            cap_add=None,
            extra_hosts=None,
            privileged=False,
            device_requests=[DeviceRequest(count=-1, capabilities=[["gpu"]])],
            log_config=LogConfig(config={"max-size": "10m", "max-file": "5"}),
            ipc_mode=None,
        )
        self.tempdir_mock.assert_called_once_with(dir="/host/airflow", prefix="airflowtmp")
        self.client_mock.images.assert_called_once_with(name="ubuntu:latest")
        self.client_mock.attach.assert_called_once_with(
            container="some_id", stdout=True, stderr=True, stream=True
        )
        self.client_mock.pull.assert_called_once_with("ubuntu:latest", stream=True, decode=True)
        self.client_mock.wait.assert_called_once_with("some_id")
        assert (
            operator.cli.pull("ubuntu:latest", stream=True, decode=True) == self.client_mock.pull.return_value
        )
        stringio_mock.assert_called_once_with("ENV=FILE\nVAR=VALUE")
        self.dotenv_mock.assert_called_once_with(stream="ENV=FILE\nVAR=VALUE")
        stringio_patcher.stop()

    def test_execute_no_temp_dir(self):
        stringio_patcher = mock.patch("airflow.providers.docker.operators.docker.StringIO")
        stringio_mock = stringio_patcher.start()
        stringio_mock.side_effect = lambda *args: args[0]

        operator = DockerOperator(
            api_version="1.19",
            command="env",
            environment={"UNIT": "TEST"},
            private_environment={"PRIVATE": "MESSAGE"},
            env_file="ENV=FILE\nVAR=VALUE",
            image="ubuntu:latest",
            network_mode="bridge",
            owner="unittest",
            task_id="unittest",
            mounts=[Mount(source="/host/path", target="/container/path", type="bind")],
            mount_tmp_dir=False,
            entrypoint='["sh", "-c"]',
            working_dir="/container/path",
            shm_size=1000,
            host_tmp_dir="/host/airflow",
            container_name="test_container",
            tty=True,
        )
        operator.execute(None)

        self.client_class_mock.assert_called_once_with(
            base_url="unix://var/run/docker.sock", tls=None, version="1.19", timeout=DEFAULT_TIMEOUT_SECONDS
        )

        self.client_mock.create_container.assert_called_once_with(
            command="env",
            name="test_container",
            environment={"UNIT": "TEST", "PRIVATE": "MESSAGE", "ENV": "FILE", "VAR": "VALUE"},
            host_config=self.client_mock.create_host_config.return_value,
            image="ubuntu:latest",
            user=None,
            entrypoint=["sh", "-c"],
            working_dir="/container/path",
            tty=True,
        )
        self.client_mock.create_host_config.assert_called_once_with(
            mounts=[
                Mount(source="/host/path", target="/container/path", type="bind"),
            ],
            network_mode="bridge",
            shm_size=1000,
            cpu_shares=1024,
            mem_limit=None,
            auto_remove=False,
            dns=None,
            dns_search=None,
            cap_add=None,
            extra_hosts=None,
            privileged=False,
            device_requests=None,
            log_config=LogConfig(config={}),
            ipc_mode=None,
        )
        self.tempdir_mock.assert_not_called()
        self.client_mock.images.assert_called_once_with(name="ubuntu:latest")
        self.client_mock.attach.assert_called_once_with(
            container="some_id", stdout=True, stderr=True, stream=True
        )
        self.client_mock.pull.assert_called_once_with("ubuntu:latest", stream=True, decode=True)
        self.client_mock.wait.assert_called_once_with("some_id")
        assert (
            operator.cli.pull("ubuntu:latest", stream=True, decode=True) == self.client_mock.pull.return_value
        )
        stringio_mock.assert_called_once_with("ENV=FILE\nVAR=VALUE")
        self.dotenv_mock.assert_called_once_with(stream="ENV=FILE\nVAR=VALUE")
        stringio_patcher.stop()

    def test_execute_fallback_temp_dir(self, caplog):
        self.client_mock.create_container.side_effect = [
            APIError(message="wrong path: " + TEMPDIR_MOCK_RETURN_VALUE),
            {"Id": "some_id"},
        ]

        stringio_patcher = mock.patch("airflow.providers.docker.operators.docker.StringIO")
        stringio_mock = stringio_patcher.start()
        stringio_mock.side_effect = lambda *args: args[0]

        operator = DockerOperator(
            api_version="1.19",
            command="env",
            environment={"UNIT": "TEST"},
            private_environment={"PRIVATE": "MESSAGE"},
            env_file="ENV=FILE\nVAR=VALUE",
            image="ubuntu:latest",
            network_mode="bridge",
            owner="unittest",
            task_id="unittest",
            mounts=[Mount(source="/host/path", target="/container/path", type="bind")],
            mount_tmp_dir=True,
            entrypoint='["sh", "-c"]',
            working_dir="/container/path",
            shm_size=1000,
            host_tmp_dir="/host/airflow",
            container_name="test_container",
            tty=True,
        )
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=operator.log.name):
            operator.execute(None)
            warning_message = (
                "Using remote engine or docker-in-docker and mounting temporary volume from host "
                "is not supported. Falling back to `mount_tmp_dir=False` mode. "
                "You can set `mount_tmp_dir` parameter to False to disable mounting and remove the warning"
            )
            assert warning_message in caplog.messages

        self.client_class_mock.assert_called_once_with(
            base_url="unix://var/run/docker.sock", tls=None, version="1.19", timeout=DEFAULT_TIMEOUT_SECONDS
        )
        self.client_mock.create_container.assert_has_calls(
            [
                call(
                    command="env",
                    name="test_container",
                    environment={
                        "AIRFLOW_TMP_DIR": "/tmp/airflow",
                        "UNIT": "TEST",
                        "PRIVATE": "MESSAGE",
                        "ENV": "FILE",
                        "VAR": "VALUE",
                    },
                    host_config=self.client_mock.create_host_config.return_value,
                    image="ubuntu:latest",
                    user=None,
                    entrypoint=["sh", "-c"],
                    working_dir="/container/path",
                    tty=True,
                ),
                call(
                    command="env",
                    name="test_container",
                    environment={"UNIT": "TEST", "PRIVATE": "MESSAGE", "ENV": "FILE", "VAR": "VALUE"},
                    host_config=self.client_mock.create_host_config.return_value,
                    image="ubuntu:latest",
                    user=None,
                    entrypoint=["sh", "-c"],
                    working_dir="/container/path",
                    tty=True,
                ),
            ]
        )
        self.client_mock.create_host_config.assert_has_calls(
            [
                call(
                    mounts=[
                        Mount(source="/host/path", target="/container/path", type="bind"),
                        Mount(source="/mkdtemp", target="/tmp/airflow", type="bind"),
                    ],
                    network_mode="bridge",
                    shm_size=1000,
                    cpu_shares=1024,
                    mem_limit=None,
                    auto_remove=False,
                    dns=None,
                    dns_search=None,
                    cap_add=None,
                    extra_hosts=None,
                    privileged=False,
                    device_requests=None,
                    log_config=LogConfig(config={}),
                    ipc_mode=None,
                ),
                call(
                    mounts=[
                        Mount(source="/host/path", target="/container/path", type="bind"),
                    ],
                    network_mode="bridge",
                    shm_size=1000,
                    cpu_shares=1024,
                    mem_limit=None,
                    auto_remove=False,
                    dns=None,
                    dns_search=None,
                    cap_add=None,
                    extra_hosts=None,
                    privileged=False,
                    device_requests=None,
                    log_config=LogConfig(config={}),
                    ipc_mode=None,
                ),
            ]
        )
        self.tempdir_mock.assert_called_once_with(dir="/host/airflow", prefix="airflowtmp")
        self.client_mock.images.assert_called_once_with(name="ubuntu:latest")
        self.client_mock.attach.assert_called_once_with(
            container="some_id", stdout=True, stderr=True, stream=True
        )
        self.client_mock.pull.assert_called_once_with("ubuntu:latest", stream=True, decode=True)
        self.client_mock.wait.assert_called_once_with("some_id")
        assert (
            operator.cli.pull("ubuntu:latest", stream=True, decode=True) == self.client_mock.pull.return_value
        )
        stringio_mock.assert_called_with("ENV=FILE\nVAR=VALUE")
        self.dotenv_mock.assert_called_with(stream="ENV=FILE\nVAR=VALUE")
        stringio_patcher.stop()

    def test_private_environment_is_private(self):
        operator = DockerOperator(
            private_environment={"PRIVATE": "MESSAGE"}, image="ubuntu:latest", task_id="unittest"
        )
        assert operator._private_environment == {
            "PRIVATE": "MESSAGE"
        }, "To keep this private, it must be an underscored attribute."

    @mock.patch("airflow.providers.docker.operators.docker.StringIO")
    def test_environment_overrides_env_file(self, stringio_mock):
        stringio_mock.side_effect = lambda *args: args[0]
        operator = DockerOperator(
            command="env",
            environment={"UNIT": "TEST"},
            private_environment={"PRIVATE": "MESSAGE"},
            env_file="UNIT=FILE\nPRIVATE=FILE\nVAR=VALUE",
            image="ubuntu:latest",
            task_id="unittest",
            entrypoint='["sh", "-c"]',
            working_dir="/container/path",
            host_tmp_dir="/host/airflow",
            container_name="test_container",
            tty=True,
        )
        operator.execute(None)
        self.client_mock.create_container.assert_called_once_with(
            command="env",
            name="test_container",
            environment={
                "AIRFLOW_TMP_DIR": "/tmp/airflow",
                "UNIT": "TEST",
                "PRIVATE": "MESSAGE",
                "VAR": "VALUE",
            },
            host_config=self.client_mock.create_host_config.return_value,
            image="ubuntu:latest",
            user=None,
            entrypoint=["sh", "-c"],
            working_dir="/container/path",
            tty=True,
        )
        stringio_mock.assert_called_once_with("UNIT=FILE\nPRIVATE=FILE\nVAR=VALUE")
        self.dotenv_mock.assert_called_once_with(stream="UNIT=FILE\nPRIVATE=FILE\nVAR=VALUE")

    @mock.patch("airflow.providers.docker.operators.docker.tls.TLSConfig")
    def test_execute_tls(self, tls_class_mock):
        tls_mock = mock.Mock()
        tls_class_mock.return_value = tls_mock

        operator = DockerOperator(
            docker_url="tcp://127.0.0.1:2376",
            image="ubuntu",
            owner="unittest",
            task_id="unittest",
            tls_client_cert="cert.pem",
            tls_ca_cert="ca.pem",
            tls_client_key="key.pem",
        )
        operator.execute(None)

        tls_class_mock.assert_called_once_with(
            assert_hostname=None,
            ca_cert="ca.pem",
            client_cert=("cert.pem", "key.pem"),
            ssl_version=None,
            verify=True,
        )

        self.client_class_mock.assert_called_once_with(
            base_url="https://127.0.0.1:2376", tls=tls_mock, version=None, timeout=DEFAULT_TIMEOUT_SECONDS
        )

    def test_execute_unicode_logs(self):
        self.client_mock.attach.return_value = ["unicode container log 😁"]

        originalRaiseExceptions = logging.raiseExceptions
        logging.raiseExceptions = True

        operator = DockerOperator(image="ubuntu", owner="unittest", task_id="unittest")

        with mock.patch("traceback.print_exception") as print_exception_mock:
            operator.execute(None)
            logging.raiseExceptions = originalRaiseExceptions
            print_exception_mock.assert_not_called()

    def test_execute_container_fails(self):
        failed_msg = {"StatusCode": 1}
        log_line = ["unicode container log 😁   ", b"byte string container log"]
        expected_message = "Docker container failed: {failed_msg} lines {expected_log_output}"
        self.client_mock.attach.return_value = log_line
        self.client_mock.wait.return_value = failed_msg

        operator = DockerOperator(image="ubuntu", owner="unittest", task_id="unittest")

        with pytest.raises(AirflowException) as raised_exception:
            operator.execute(None)

        assert str(raised_exception.value) == expected_message.format(
            failed_msg=failed_msg,
            expected_log_output=f'{log_line[0].strip()}\n{log_line[1].decode("utf-8")}',
        )

    def test_auto_remove_container_fails(self):
        self.client_mock.wait.return_value = {"StatusCode": 1}
        operator = DockerOperator(image="ubuntu", owner="unittest", task_id="unittest", auto_remove=True)
        operator.container = {"Id": "some_id"}
        with pytest.raises(AirflowException):
            operator.execute(None)

        self.client_mock.remove_container.assert_called_once_with("some_id")

    @staticmethod
    def test_on_kill():
        client_mock = mock.Mock(spec=APIClient)

        operator = DockerOperator(image="ubuntu", owner="unittest", task_id="unittest")
        operator.cli = client_mock
        operator.container = {"Id": "some_id"}

        operator.on_kill()

        client_mock.stop.assert_called_once_with("some_id")

    def test_execute_no_docker_conn_id_no_hook(self):
        # Create the DockerOperator
        operator = DockerOperator(image="publicregistry/someimage", owner="unittest", task_id="unittest")

        # Mock out the DockerHook
        hook_mock = mock.Mock(name="DockerHook mock", spec=DockerHook)
        hook_mock.get_conn.return_value = self.client_mock
        operator.get_hook = mock.Mock(
            name="DockerOperator.get_hook mock", spec=DockerOperator.get_hook, return_value=hook_mock
        )

        operator.execute(None)
        assert operator.get_hook.call_count == 0, "Hook called though no docker_conn_id configured"

    @mock.patch("airflow.providers.docker.operators.docker.DockerHook")
    def test_execute_with_docker_conn_id_use_hook(self, hook_class_mock):
        # Create the DockerOperator
        operator = DockerOperator(
            image="publicregistry/someimage",
            owner="unittest",
            task_id="unittest",
            docker_conn_id="some_conn_id",
        )

        # Mock out the DockerHook
        hook_mock = mock.Mock(name="DockerHook mock", spec=DockerHook)
        hook_mock.get_conn.return_value = self.client_mock
        hook_class_mock.return_value = hook_mock

        operator.execute(None)

        assert self.client_class_mock.call_count == 0, "Client was called on the operator instead of the hook"
        assert hook_class_mock.call_count == 1, "Hook was not called although docker_conn_id configured"
        assert self.client_mock.pull.call_count == 1, "Image was not pulled using operator client"

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
        assert "host_config" in self.client_mock.create_container.call_args[1]
        assert "extra_hosts" in self.client_mock.create_host_config.call_args[1]
        assert hosts_obj is self.client_mock.create_host_config.call_args[1]["extra_hosts"]

    def test_privileged(self):
        privileged = mock.Mock()
        operator = DockerOperator(task_id="test", image="test", privileged=privileged)
        operator.execute(None)
        self.client_mock.create_container.assert_called_once()
        assert "host_config" in self.client_mock.create_container.call_args[1]
        assert "privileged" in self.client_mock.create_host_config.call_args[1]
        assert privileged is self.client_mock.create_host_config.call_args[1]["privileged"]
