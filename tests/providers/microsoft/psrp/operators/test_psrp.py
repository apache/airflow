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

import unittest
from unittest.mock import patch

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.psrp.operators.psrp import PSRPOperator

CONNECTION_ID = "conn_id"


class TestPSRPOperator(unittest.TestCase):
    def test_no_command_or_powershell(self):
        exception_msg = "Must provide either 'command' or 'powershell'"
        with pytest.raises(ValueError, match=exception_msg):
            PSRPOperator(task_id='test_task_id', psrp_conn_id=CONNECTION_ID)

    @parameterized.expand(
        [
            (False,),
            (True,),
        ]
    )
    @patch(f"{PSRPOperator.__module__}.PSRPHook")
    def test_execute(self, had_errors, hook):
        op = PSRPOperator(task_id='test_task_id', psrp_conn_id=CONNECTION_ID, command='dummy')
        ps = hook.return_value.__enter__.return_value.invoke_powershell.return_value
        ps.output = ["<output>"]
        ps.had_errors = had_errors
        if had_errors:
            exception_msg = "Process failed"
            with pytest.raises(AirflowException, match=exception_msg):
                op.execute(None)
        else:
            output = op.execute(None)
            assert output == ps.output
