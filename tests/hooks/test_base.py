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

import os
from unittest import mock

import pytest

from airflow.exceptions import AirflowClusterPolicyViolation
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from tests import cluster_policies


class TestBaseHook:
    def test_hook_has_default_logger_name(self):
        hook = BaseHook()
        assert hook.log.name == "airflow.task.hooks.airflow.hooks.base.BaseHook"

    def test_custom_logger_name_is_correctly_set(self):
        hook = BaseHook(logger_name="airflow.custom.logger")
        assert hook.log.name == "airflow.task.hooks.airflow.custom.logger"

    def test_empty_string_as_logger_name(self):
        hook = BaseHook(logger_name="")
        assert hook.log.name == "airflow.task.hooks"

    @mock.patch.dict(os.environ, {"AIRFLOW_CONN_BI/TEST_CONN": "foobar://"})
    @mock.patch("airflow.settings.connection_policy", cluster_policies.connection_policy)
    def test_connection_policy(self, dag_maker):
        """Test whether the connection id is modified successfully according to the connection policy."""
        with dag_maker(dag_id="test", default_args={"owner": "bi"}):

            def do_stuff():
                BaseHook.get_connection(conn_id="test_conn")

            test_task = PythonOperator(task_id="test", python_callable=do_stuff)

        dr = dag_maker.create_dagrun()
        test_task.run(start_date=dr.execution_date, end_date=dr.execution_date)

    @mock.patch("airflow.settings.connection_policy", cluster_policies.connection_policy)
    def test_connection_policy_failure(self, dag_maker):
        """
        Test if an error is raised according to the connection policy (which doesn't know owner 'unknown').
        """
        with dag_maker(dag_id="test", default_args={"owner": "unknown"}):

            def do_stuff():
                BaseHook.get_connection(conn_id="test_conn")

            test_task = PythonOperator(task_id="test", python_callable=do_stuff)

        dr = dag_maker.create_dagrun()
        with pytest.raises(AirflowClusterPolicyViolation):
            test_task.run(start_date=dr.execution_date, end_date=dr.execution_date)
