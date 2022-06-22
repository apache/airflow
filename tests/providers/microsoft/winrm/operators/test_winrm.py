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
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator


class TestWinRMOperator(unittest.TestCase):
    def test_no_winrm_hook_no_ssh_conn_id(self):
        op = WinRMOperator(task_id='test_task_id', winrm_hook=None, ssh_conn_id=None)
        exception_msg = "Cannot operate without winrm_hook or ssh_conn_id."
        with pytest.raises(AirflowException, match=exception_msg):
            op.execute(None)

    @mock.patch('airflow.providers.microsoft.winrm.operators.winrm.WinRMHook')
    def test_no_command(self, mock_hook):
        op = WinRMOperator(task_id='test_task_id', winrm_hook=mock_hook, command=None)
        exception_msg = "No command specified so nothing to execute here."
        with pytest.raises(AirflowException, match=exception_msg):
            op.execute(None)
