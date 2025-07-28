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

from collections.abc import Generator
from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.operators.ssm import SsmRunCommandOperator

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

COMMAND_ID = "test_command_id"
DOCUMENT_NAME = "test_ssm_custom_document"
INSTANCE_IDS = ["test_instance_id_1", "test_instance_id_2"]


class TestSsmRunCommandOperator:
    @pytest.fixture
    def mock_conn(self) -> Generator[SsmHook, None, None]:
        with mock.patch.object(SsmHook, "conn") as _conn:
            _conn.send_command.return_value = {
                "Command": {
                    "CommandId": COMMAND_ID,
                    "InstanceIds": INSTANCE_IDS,
                }
            }
            yield _conn

    def setup_method(self):
        self.operator = SsmRunCommandOperator(
            task_id="test_run_command_operator",
            document_name=DOCUMENT_NAME,
            run_command_kwargs={"InstanceIds": INSTANCE_IDS},
        )
        self.operator.defer = mock.MagicMock()

    @pytest.mark.parametrize(
        "wait_for_completion, deferrable",
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(SsmHook, "get_waiter")
    def test_run_command_wait_combinations(self, mock_get_waiter, wait_for_completion, deferrable, mock_conn):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        command_id = self.operator.execute({})

        assert command_id == COMMAND_ID
        mock_conn.send_command.assert_called_once_with(DocumentName=DOCUMENT_NAME, InstanceIds=INSTANCE_IDS)
        assert mock_get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    def test_template_fields(self):
        validate_template_fields(self.operator)
