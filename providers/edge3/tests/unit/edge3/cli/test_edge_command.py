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

from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.edge3.cli import edge_command


class TestWorkerAdminExitCodes:
    """CLI worker-admin subcommands must exit non-zero when the model raises."""

    @pytest.fixture(autouse=True)
    def _stub_db_checks(self):
        with (
            mock.patch.object(edge_command, "_check_valid_db_connection"),
            mock.patch.object(edge_command, "_check_if_registered_edge_host"),
        ):
            yield

    @pytest.mark.parametrize(
        ("cli_func", "model_attr", "args"),
        [
            (
                edge_command.add_worker_queues,
                "add_worker_queues",
                SimpleNamespace(edge_hostname="worker-1", queues="q1,q2"),
            ),
            (
                edge_command.remove_worker_queues,
                "remove_worker_queues",
                SimpleNamespace(edge_hostname="worker-1", queues="q1"),
            ),
            (
                edge_command.set_remote_worker_concurrency,
                "set_worker_concurrency",
                SimpleNamespace(edge_hostname="worker-1", concurrency=8),
            ),
        ],
        ids=["add_worker_queues", "remove_worker_queues", "set_remote_worker_concurrency"],
    )
    def test_exits_non_zero_when_model_raises_type_error(self, cli_func, model_attr, args):
        message = "Cannot mutate worker in OFFLINE state!"
        with mock.patch(
            f"airflow.providers.edge3.models.edge_worker.{model_attr}",
            side_effect=TypeError(message),
        ):
            with pytest.raises(SystemExit) as exc_info:
                cli_func(args)
        # SystemExit with a string argument yields exit code 1 via sys.exit's
        # convention; a bare `raise SystemExit` would leave code=None (exit 0).
        assert exc_info.value.code == message
