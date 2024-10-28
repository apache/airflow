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

import pytest
from docker import APIClient, types
from docker.constants import DEFAULT_TIMEOUT_SECONDS
from docker.errors import APIError

from airflow.exceptions import AirflowException
from airflow.providers.docker.operators.docker_swarm import DockerSwarmOperator


class TestDockerSwarmOperator:
    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_execute(self, types_mock, docker_api_client_patcher, caplog):
        mock_obj = mock.Mock()

        def _client_tasks_side_effect():
            for _ in range(2):
                yield [{"Status": {"State": "pending"}}]
            while True:
                yield [
                    {
                        "Status": {
                            "State": "complete",
                            "ContainerStatus": {"ContainerID": "some_id"},
                        }
                    }
                ]

        def _client_service_logs_effect():
            service_logs = [
                b"2024-07-09T19:07:04.587918327Z lineone-one\rlineone-two",
                b"2024-07-09T19:07:04.587918328Z linetwo",
            ]
            return (log for log in service_logs)

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.service_logs.return_value = _client_service_logs_effect()
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.side_effect = _client_tasks_side_effect()
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            api_version="1.19",
            command="env",
            environment={"UNIT": "TEST"},
            image="ubuntu:latest",
            mem_limit="128m",
            user="unittest",
            task_id="unittest",
            mounts=[
                types.Mount(source="/host/path", target="/container/path", type="bind")
            ],
            auto_remove="success",
            tty=True,
            configs=[
                types.ConfigReference(
                    config_id="dummy_cfg_id", config_name="dummy_cfg_name"
                )
            ],
            secrets=[
                types.SecretReference(
                    secret_id="dummy_secret_id", secret_name="dummy_secret_name"
                )
            ],
            mode=types.ServiceMode(mode="replicated", replicas=3),
            networks=["dummy_network"],
            placement=types.Placement(constraints=["node.labels.region==east"]),
            logging_driver=None,
            logging_driver_opts=None,
        )
        caplog.clear()
        operator.execute(None)

        types_mock.TaskTemplate.assert_called_once_with(
            container_spec=mock_obj,
            restart_policy=mock_obj,
            resources=mock_obj,
            networks=["dummy_network"],
            placement=types.Placement(constraints=["node.labels.region==east"]),
            log_driver=None,
        )
        types_mock.ContainerSpec.assert_called_once_with(
            image="ubuntu:latest",
            command="env",
            args=None,
            user="unittest",
            mounts=[
                types.Mount(source="/host/path", target="/container/path", type="bind")
            ],
            tty=True,
            env={"UNIT": "TEST", "AIRFLOW_TMP_DIR": "/tmp/airflow"},
            configs=[
                types.ConfigReference(
                    config_id="dummy_cfg_id", config_name="dummy_cfg_name"
                )
            ],
            secrets=[
                types.SecretReference(
                    secret_id="dummy_secret_id", secret_name="dummy_secret_name"
                )
            ],
        )
        types_mock.RestartPolicy.assert_called_once_with(condition="none")
        types_mock.Resources.assert_called_once_with(mem_limit="128m")

        docker_api_client_patcher.assert_called_once_with(
            base_url="unix://var/run/docker.sock",
            tls=False,
            version="1.19",
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )

        client_mock.service_logs.assert_called_with(
            "some_id",
            follow=False,
            stdout=True,
            stderr=True,
            is_tty=True,
            since=0,
            timestamps=True,
        )

        with caplog.at_level(logging.INFO, logger=operator.log.name):
            service_logs = [
                "lineone-one\rlineone-two",
                "linetwo",
            ]
            for log_line in service_logs:
                assert log_line in caplog.messages

        csargs, cskwargs = client_mock.create_service.call_args_list[0]
        assert (
            len(csargs) == 1
        ), "create_service called with different number of arguments than expected"
        assert csargs == (mock_obj,)
        assert cskwargs["labels"] == {"name": "airflow__adhoc_airflow__unittest"}
        assert cskwargs["name"].startswith("airflow-")
        assert cskwargs["mode"] == types.ServiceMode(mode="replicated", replicas=3)
        assert client_mock.tasks.call_count == 8
        client_mock.remove_service.assert_called_once_with("some_id")

    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_auto_remove(self, types_mock, docker_api_client_patcher):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [
            {
                "Status": {
                    "State": "complete",
                    "ContainerStatus": {"ContainerID": "some_id"},
                }
            }
        ]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            image="", auto_remove="success", task_id="unittest", enable_logging=False
        )
        operator.execute(None)

        client_mock.remove_service.assert_called_once_with("some_id")

    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_no_auto_remove(self, types_mock, docker_api_client_patcher):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [
            {
                "Status": {
                    "State": "complete",
                    "ContainerStatus": {"ContainerID": "some_id"},
                }
            }
        ]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            image="", auto_remove="never", task_id="unittest", enable_logging=False
        )
        operator.execute(None)

        assert (
            client_mock.remove_service.call_count == 0
        ), "Docker service being removed even when `auto_remove` set to `never`"

    @pytest.mark.parametrize(
        "status", ["failed", "shutdown", "rejected", "orphaned", "remove"]
    )
    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_non_complete_service_raises_error(
        self, types_mock, docker_api_client_patcher, status
    ):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [{"Status": {"State": status}}]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            image="", auto_remove="never", task_id="unittest", enable_logging=False
        )
        msg = "Service did not complete: {'ID': 'some_id'}"
        with pytest.raises(AirflowException) as ctx:
            operator.execute(None)
        assert str(ctx.value) == msg

    @pytest.mark.parametrize("service_exists", [True, False])
    def test_on_kill_client_created(self, docker_api_client_patcher, service_exists):
        """Test operator on_kill method if APIClient created."""
        op = DockerSwarmOperator(image="", task_id="test_on_kill")
        op.service = {"ID": "some_id"} if service_exists else None

        op.hook.get_conn()  # Try to create APIClient
        op.on_kill()
        if service_exists:
            docker_api_client_patcher.return_value.remove_service.assert_called_once_with(
                "some_id"
            )
        else:
            docker_api_client_patcher.return_value.remove_service.assert_not_called()

    def test_on_kill_client_not_created(self, docker_api_client_patcher):
        """Test operator on_kill method if APIClient not created in case of error."""
        docker_api_client_patcher.side_effect = APIError("Fake Client Error")
        op = DockerSwarmOperator(image="", task_id="test_on_kill")
        mock_service = mock.MagicMock()
        op.service = mock_service

        with pytest.raises(APIError, match="Fake Client Error"):
            op.hook.get_conn()
        op.on_kill()
        docker_api_client_patcher.return_value.remove_service.assert_not_called()
        mock_service.assert_not_called()

    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_container_resources(self, types_mock, docker_api_client_patcher):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [
            {
                "Status": {
                    "State": "complete",
                    "ContainerStatus": {"ContainerID": "some_id"},
                }
            }
        ]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            image="ubuntu:latest",
            task_id="unittest",
            auto_remove="success",
            enable_logging=False,
            mem_limit="128m",
            container_resources=types.Resources(
                cpu_limit=250000000,
                mem_limit=67108864,
                cpu_reservation=100000000,
                mem_reservation=67108864,
            ),
            logging_driver=None,
            logging_driver_opts=None,
        )
        operator.execute(None)

        types_mock.TaskTemplate.assert_called_once_with(
            container_spec=mock_obj,
            restart_policy=mock_obj,
            resources=types.Resources(
                cpu_limit=250000000,
                mem_limit=67108864,
                cpu_reservation=100000000,
                mem_reservation=67108864,
            ),
            networks=None,
            placement=None,
            log_driver=None,
        )
        types_mock.Resources.assert_not_called()

    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_service_args_str(self, types_mock, docker_api_client_patcher):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [
            {
                "Status": {
                    "State": "complete",
                    "ContainerStatus": {"ContainerID": "some_id"},
                }
            }
        ]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            image="ubuntu:latest",
            command="env",
            args="--show",
            task_id="unittest",
            auto_remove="success",
            enable_logging=False,
        )
        operator.execute(None)

        types_mock.ContainerSpec.assert_called_once_with(
            image="ubuntu:latest",
            command="env",
            args=["--show"],
            user=None,
            mounts=[],
            tty=False,
            env={"AIRFLOW_TMP_DIR": "/tmp/airflow"},
            configs=None,
            secrets=None,
        )

    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_service_args_list(self, types_mock, docker_api_client_patcher):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [
            {
                "Status": {
                    "State": "complete",
                    "ContainerStatus": {"ContainerID": "some_id"},
                }
            }
        ]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            image="ubuntu:latest",
            command="env",
            args=["--show"],
            task_id="unittest",
            auto_remove="success",
            enable_logging=False,
        )
        operator.execute(None)

        types_mock.ContainerSpec.assert_called_once_with(
            image="ubuntu:latest",
            command="env",
            args=["--show"],
            user=None,
            mounts=[],
            tty=False,
            env={"AIRFLOW_TMP_DIR": "/tmp/airflow"},
            configs=None,
            secrets=None,
        )

    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_logging_driver(self, types_mock, docker_api_client_patcher):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [{"Status": {"State": "complete"}}]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        operator = DockerSwarmOperator(
            image="", logging_driver="json-file", task_id="unittest", enable_logging=False
        )

        assert operator.logging_driver == "json-file"

    @mock.patch("airflow.providers.docker.operators.docker_swarm.types")
    def test_invalid_logging_driver(self, types_mock, docker_api_client_patcher):
        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {"ID": "some_id"}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [{"Status": {"State": "complete"}}]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        docker_api_client_patcher.return_value = client_mock

        msg = "Invalid logging driver provided: json. Must be one of: [json-file, gelf]"
        with pytest.raises(AirflowException) as e:
            # Exception is raised in __init__()
            DockerSwarmOperator(
                image="", logging_driver="json", task_id="unittest", enable_logging=False
            )
        assert str(e.value) == msg
