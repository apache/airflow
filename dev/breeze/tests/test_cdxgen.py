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

import signal
from unittest import mock

import pytest

from airflow_breeze.utils import cdxgen

RM_SERVER = mock.call(
    ["docker", "rm", "--force", "airflow-cdxgen-server-8081"], check=False, capture_output=True, quiet=True
)


@mock.patch.object(cdxgen.atexit, "register", autospec=True)
@mock.patch.object(cdxgen, "run_command", autospec=True)
@mock.patch.object(cdxgen.os, "fork", autospec=True, return_value=1234)
def test_parent_removes_the_server_container_after_terminating_docker_run(
    mock_fork, mock_run_command, mock_register
):
    cdxgen.fork_cdxgen_server("/app", port=8081)

    # A container left by an earlier run would hold the name and the port.
    assert mock_run_command.call_args_list == [RM_SERVER]
    # atexit calls the handlers in reverse order of registration.
    assert mock_register.call_args_list == [
        mock.call(cdxgen.remove_cdxgen_server, "airflow-cdxgen-server-8081"),
        mock.call(cdxgen.os.killpg, 1234, signal.SIGTERM),
    ]


class ChildExit(Exception):
    pass


@pytest.mark.parametrize(
    "docker_run_error",
    [pytest.param(None, id="server-terminated"), pytest.param(OSError("failed"), id="server-failed")],
)
@mock.patch.object(cdxgen.os, "_exit", autospec=True, side_effect=ChildExit)
@mock.patch.object(cdxgen.os, "setpgid", autospec=True)
@mock.patch.object(cdxgen, "run_command", autospec=True)
@mock.patch.object(cdxgen.os, "fork", autospec=True, return_value=0)
def test_child_runs_the_server_container_and_skips_inherited_exit_handlers(
    mock_fork, mock_run_command, mock_setpgid, mock_exit, docker_run_error
):
    mock_run_command.side_effect = [mock.DEFAULT, docker_run_error or mock.DEFAULT]

    # os._exit, unlike sys.exit, does not run the atexit handlers inherited from the parent,
    # which would stop the servers forked earlier.
    with pytest.raises(ChildExit):
        cdxgen.fork_cdxgen_server("/app", port=8081)

    mock_exit.assert_called_once_with(0)
    docker_run = mock_run_command.call_args_list[-1].args[0]
    assert docker_run[:2] == ["docker", "run"]
    assert docker_run[docker_run.index("--name") + 1] == "airflow-cdxgen-server-8081"
