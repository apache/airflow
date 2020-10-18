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

import mock

from airflow.providers.exasol.operators.exasol import ExasolOperator


class TestExasol(unittest.TestCase):
    @mock.patch('airflow.providers.exasol.hooks.exasol.ExasolHook.run')
    def test_overwrite_autocommit(self, mock_run):
        operator = ExasolOperator(task_id='TEST', sql='SELECT 1', autocommit=True)
        operator.execute({})
        mock_run.assert_called_once_with('SELECT 1', autocommit=True, parameters=None)

    @mock.patch('airflow.providers.exasol.hooks.exasol.ExasolHook.run')
    def test_pass_parameters(self, mock_run):
        operator = ExasolOperator(task_id='TEST', sql='SELECT {value!s}', parameters={'value': 1})
        operator.execute({})
        mock_run.assert_called_once_with('SELECT {value!s}', autocommit=False, parameters={'value': 1})

    @mock.patch('airflow.providers.exasol.operators.exasol.ExasolHook')
    def test_overwrite_schema(self, mock_hook):
        operator = ExasolOperator(task_id='TEST', sql='SELECT 1', schema='dummy')
        operator.execute({})
        mock_hook.assert_called_once_with(exasol_conn_id='exasol_default', schema='dummy')
