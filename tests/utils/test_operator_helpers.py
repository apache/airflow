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
from datetime import datetime
from unittest import mock

from airflow.utils import operator_helpers


class TestOperatorHelpers(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.dag_id = 'dag_id'
        self.task_id = 'task_id'
        self.execution_date = '2017-05-21T00:00:00'
        self.dag_run_id = 'dag_run_id'
        self.owner = ['owner1', 'owner2']
        self.email = ['email1@test.com']
        self.context = {
            'dag_run': mock.MagicMock(
                name='dag_run',
                run_id=self.dag_run_id,
                execution_date=datetime.strptime(self.execution_date, '%Y-%m-%dT%H:%M:%S'),
            ),
            'task_instance': mock.MagicMock(
                name='task_instance',
                task_id=self.task_id,
                dag_id=self.dag_id,
                execution_date=datetime.strptime(self.execution_date, '%Y-%m-%dT%H:%M:%S'),
            ),
            'task': mock.MagicMock(name='task', owner=self.owner, email=self.email),
        }

    def test_context_to_airflow_vars_empty_context(self):
        self.assertDictEqual(operator_helpers.context_to_airflow_vars({}), {})

    def test_context_to_airflow_vars_all_context(self):
        self.assertDictEqual(
            operator_helpers.context_to_airflow_vars(self.context),
            {
                'airflow.ctx.dag_id': self.dag_id,
                'airflow.ctx.execution_date': self.execution_date,
                'airflow.ctx.task_id': self.task_id,
                'airflow.ctx.dag_run_id': self.dag_run_id,
                'airflow.ctx.dag_owner': 'owner1,owner2',
                'airflow.ctx.dag_email': 'email1@test.com',
            },
        )

        self.assertDictEqual(
            operator_helpers.context_to_airflow_vars(self.context, in_env_var_format=True),
            {
                'AIRFLOW_CTX_DAG_ID': self.dag_id,
                'AIRFLOW_CTX_EXECUTION_DATE': self.execution_date,
                'AIRFLOW_CTX_TASK_ID': self.task_id,
                'AIRFLOW_CTX_DAG_RUN_ID': self.dag_run_id,
                'AIRFLOW_CTX_DAG_OWNER': 'owner1,owner2',
                'AIRFLOW_CTX_DAG_EMAIL': 'email1@test.com',
            },
        )
