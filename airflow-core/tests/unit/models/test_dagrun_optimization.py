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

import unittest
from unittest import mock

from sqlalchemy.orm import Session

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance as TI


class TestDagRunOptimization(unittest.TestCase):
    def test_get_task_instances_avoids_joinedload(self):
        """
        Verify that get_task_instances calls _fetch_task_instances with load_dag_run=False
        and does NOT add joinedload(TI.dag_run) to the query.
        """
        dag_run = DagRun(dag_id="test_dag", run_id="test_run")
        session = mock.Mock(spec=Session)

        # Mock the scalars().all() return value
        ti_mock = mock.Mock(spec=TI)
        session.scalars.return_value.all.return_value = [ti_mock]

        with mock.patch("airflow.models.dagrun.select") as mock_select, \
             mock.patch("airflow.models.dagrun.joinedload") as mock_joinedload:

            # Setup the chain of mock calls for select
            mock_query = mock_select.return_value
            mock_query.where.return_value = mock_query
            mock_query.order_by.return_value = mock_query
            mock_query.options.return_value = mock_query

            # Call the method under test
            tis = dag_run.get_task_instances(session=session)

            # Verify results
            assert tis == [ti_mock]
            assert ti_mock.dag_run == dag_run  # Verify dag_run is set manually

            # Verify joinedload was NOT called/used
            mock_joinedload.assert_not_called()
            mock_query.options.assert_not_called()

    def test_fetch_task_instances_uses_joinedload(self):
        """
        Verify that fetch_task_instances calls _fetch_task_instances with load_dag_run=True
        and DOES add joinedload(TI.dag_run) to the query.
        """
        session = mock.Mock(spec=Session)

        with mock.patch("airflow.models.dagrun.select") as mock_select, \
             mock.patch("airflow.models.dagrun.joinedload") as mock_joinedload:

            # Setup the chain of mock calls for select
            mock_query = mock_select.return_value
            mock_query.where.return_value = mock_query
            mock_query.order_by.return_value = mock_query
            mock_query.options.return_value = mock_query

            # Call the method under test
            DagRun.fetch_task_instances(dag_id="test_dag", run_id="test_run", session=session)

            # Verify joinedload WAS called
            mock_joinedload.assert_called_with(TI.dag_run)
            mock_query.options.assert_called()
