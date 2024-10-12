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

from datetime import datetime, timedelta, timezone

import pytest

from airflow.exceptions import DuplicateTaskIdFound
from airflow.models.param import Param, ParamsDict
from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.definitions.dag import DAG

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)


class TestDag:
    def test_dag_topological_sort_dag_without_tasks(self):
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        assert () == dag.topological_sort()

    def test_dag_naive_start_date_string(self):
        DAG("DAG", schedule=None, default_args={"start_date": "2019-06-01"})

    def test_dag_naive_start_end_dates_strings(self):
        DAG("DAG", schedule=None, default_args={"start_date": "2019-06-01", "end_date": "2019-06-05"})

    def test_dag_start_date_propagates_to_end_date(self):
        """
        Tests that a start_date string with a timezone and an end_date string without a timezone
        are accepted and that the timezone from the start carries over the end

        This test is a little indirect, it works by setting start and end equal except for the
        timezone and then testing for equality after the DAG construction.  They'll be equal
        only if the same timezone was applied to both.

        An explicit check the `tzinfo` attributes for both are the same is an extra check.
        """
        dag = DAG(
            "DAG",
            schedule=None,
            default_args={"start_date": "2019-06-05T00:00:00+05:00", "end_date": "2019-06-05T00:00:00"},
        )
        assert dag.default_args["start_date"] == dag.default_args["end_date"]
        assert dag.default_args["start_date"].tzinfo == dag.default_args["end_date"].tzinfo

    def test_dag_as_context_manager(self):
        """
        Test DAG as a context manager.
        When used as a context manager, Operators are automatically added to
        the DAG (unless they specify a different DAG)
        """
        dag = DAG("dag", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner1"})
        dag2 = DAG("dag2", schedule=None, start_date=DEFAULT_DATE, default_args={"owner": "owner2"})

        with dag:
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2", dag=dag2)

        assert op1.dag is dag
        assert op1.owner == "owner1"
        assert op2.dag is dag2
        assert op2.owner == "owner2"

        with dag2:
            op3 = BaseOperator(task_id="op3")

        assert op3.dag is dag2
        assert op3.owner == "owner2"

        with dag:
            with dag2:
                op4 = BaseOperator(task_id="op4")
            op5 = BaseOperator(task_id="op5")

        assert op4.dag is dag2
        assert op5.dag is dag
        assert op4.owner == "owner2"
        assert op5.owner == "owner1"

        with DAG("creating_dag_in_cm", schedule=None, start_date=DEFAULT_DATE) as dag:
            BaseOperator(task_id="op6")

        assert dag.dag_id == "creating_dag_in_cm"
        assert dag.tasks[0].task_id == "op6"

        with dag:
            with dag:
                op7 = BaseOperator(task_id="op7")
            op8 = BaseOperator(task_id="op8")
        op9 = BaseOperator(task_id="op8")
        op9.dag = dag2

        assert op7.dag == dag
        assert op8.dag == dag
        assert op9.dag == dag2

    def test_params_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = DAG("test-dag", schedule=None)

        assert isinstance(dag.params, ParamsDict)
        assert 0 == len(dag.params)

    def test_params_passed_and_params_in_default_args_no_override(self):
        """
        Test that when 'params' exists as a key passed to the default_args dict
        in addition to params being passed explicitly as an argument to the
        dag, that the 'params' key of the default_args dict is merged with the
        dict of the params argument.
        """
        params1 = {"parameter1": 1}
        params2 = {"parameter2": 2}

        dag = DAG("test-dag", schedule=None, default_args={"params": params1}, params=params2)

        assert params1["parameter1"] == dag.params["parameter1"]
        assert params2["parameter2"] == dag.params["parameter2"]

    def test_not_none_schedule_with_non_default_params(self):
        """
        Test if there is a DAG with a schedule and have some params that don't have a default value raise a
        error while DAG parsing. (Because we can't schedule them if there we don't know what value to use)
        """
        params = {"param1": Param(type="string")}

        with pytest.raises(ValueError):
            DAG("my-dag", schedule=timedelta(days=1), start_date=DEFAULT_DATE, params=params)

    def test_roots(self):
        """Verify if dag.roots returns the root tasks of a DAG."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            op2 = BaseOperator(task_id="t2")
            op3 = BaseOperator(task_id="t3")
            op4 = BaseOperator(task_id="t4")
            op5 = BaseOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.roots) == {op1, op2}

    def test_leaves(self):
        """Verify if dag.leaves returns the leaf tasks of a DAG."""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            op2 = BaseOperator(task_id="t2")
            op3 = BaseOperator(task_id="t3")
            op4 = BaseOperator(task_id="t4")
            op5 = BaseOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.leaves) == {op4, op5}

    def test_duplicate_task_ids_not_allowed_with_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = BaseOperator(task_id="t1")
            with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
                BaseOperator(task_id="t1")

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_not_allowed_without_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        dag = DAG("test_dag", schedule=None, start_date=DEFAULT_DATE)
        op1 = BaseOperator(task_id="t1", dag=dag)
        with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
            BaseOperator(task_id="t1", dag=dag)

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_for_same_task_is_allowed(self):
        """Verify that same tasks with Duplicate task_id do not raise error"""
        with DAG("test_dag", schedule=None, start_date=DEFAULT_DATE) as dag:
            op1 = op2 = BaseOperator(task_id="t1")
            op3 = BaseOperator(task_id="t3")
            op1 >> op3
            op2 >> op3

        assert op1 == op2
        assert dag.task_dict == {op1.task_id: op1, op3.task_id: op3}
        assert dag.task_dict == {op2.task_id: op2, op3.task_id: op3}

    def test_fail_dag_when_schedule_is_non_none_and_empty_start_date(self):
        # Check that we get a ValueError 'start_date' for self.start_date when schedule is non-none
        with pytest.raises(ValueError, match="start_date is required when catchup=True"):
            DAG(dag_id="dag_with_non_none_schedule_and_empty_start_date", schedule="@hourly", catchup=True)
