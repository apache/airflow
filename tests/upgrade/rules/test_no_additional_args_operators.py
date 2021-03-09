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


import textwrap
from tempfile import NamedTemporaryFile
from unittest import TestCase

import pytest

from airflow.upgrade.rules.no_additional_args_in_operators import NoAdditionalArgsInOperatorsRule


class TestNoAdditionalArgsInOperatorsRule(TestCase):

    @pytest.mark.filterwarnings('always')
    def test_check(self):
        rule = NoAdditionalArgsInOperatorsRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        with NamedTemporaryFile(mode='w', suffix='.py') as dag_file:
            dag_file.write(
                textwrap.dedent('''
            from airflow import DAG
            from airflow.utils.dates import days_ago
            from airflow.operators.bash_operator import BashOperator

            with DAG(dag_id="test_no_additional_args_operators", start_date=days_ago(0)):
                BashOperator(task_id='test', bash_command="true", extra_param=42)
                '''))
            dag_file.flush()
            msgs = list(rule.check(dags_folder=dag_file.name))
            assert len(msgs) == 1
