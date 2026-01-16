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

from airflow.providers.microsoft.winrm.triggers.winrm import WinRMCommandOutputTrigger


class TestWinRMCommandOutputTrigger:
    def test_serialize(self):
        trigger = WinRMCommandOutputTrigger(
            ssh_conn_id="ssh_conn_id",
            shell_id="043E496C-A9E5-4284-AFCC-78A90E2BCB65",
            command_id="E4C36903-E59F-43AB-9374-ABA87509F46D",
            output_encoding="utf-8",
            return_output=True,
            working_directory="C:\\Temp",
            expected_return_code=0,
            poll_interval=10,
            timeout=300,
            deadline=None,
        )

        actual = trigger.serialize()

        assert isinstance(actual, tuple)
        assert actual[0] == f"{WinRMCommandOutputTrigger.__module__}.{WinRMCommandOutputTrigger.__name__}"
        assert actual[1] == {
            "ssh_conn_id": "ssh_conn_id",
            "shell_id": "043E496C-A9E5-4284-AFCC-78A90E2BCB65",
            "command_id": "E4C36903-E59F-43AB-9374-ABA87509F46D",
            "output_encoding": "utf-8",
            "return_output": True,
            "working_directory": "C:\\Temp",
            "expected_return_code": 0,
            "poll_interval": 10,
            "timeout": 300,
            "deadline": None,
        }
