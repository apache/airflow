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
"""Test that DAGs with Param strftime dates can be processed without serialization errors."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag


@pytest.mark.db_test
def test_dag_validation_param_strftime_success():
    test_dag_path = Path(__file__).parent.parent / "dags" / "test_dag_validation_param_strftime.py"
    dagbag = DagBag(dag_folder=os.fspath(test_dag_path.parent), include_examples=False)

    assert "test_param_strftime_dates" in dagbag.dag_ids

    dag_file = os.fspath(test_dag_path)
    assert dag_file not in dagbag.import_errors, (
        f"Our DAG failed to load: {dagbag.import_errors.get(dag_file)}"
    )

    dag = dagbag.get_dag("test_param_strftime_dates")
    assert dag is not None
    assert dag.dag_id == "test_param_strftime_dates"

    assert "efun_query_start_date" in dag.params
    assert "efun_query_end_date" in dag.params
    assert "start_timestamp" in dag.params
    assert "end_timestamp" in dag.params

    start_date_value = dag.params["efun_query_start_date"]
    assert isinstance(start_date_value, str)
    assert "-" in start_date_value

    end_date_value = dag.params["efun_query_end_date"]
    assert isinstance(end_date_value, str)
    assert "-" in end_date_value

    start_timestamp_value = dag.params["start_timestamp"]
    assert isinstance(start_timestamp_value, int)

    end_timestamp_value = dag.params["end_timestamp"]
    assert isinstance(end_timestamp_value, int)


@pytest.mark.db_test
def test_dag_validation_param_strftime_serialization():
    from airflow.serialization.serialized_objects import SerializedDAG

    test_dag_path = Path(__file__).parent.parent / "dags" / "test_dag_validation_param_strftime.py"
    dagbag = DagBag(dag_folder=os.fspath(test_dag_path.parent), include_examples=False)

    dag = dagbag.get_dag("test_param_strftime_dates")

    serialized_dag = SerializedDAG.to_dict(dag)
    assert serialized_dag is not None
    assert "dag" in serialized_dag

    serialized_params = serialized_dag["dag"]["params"]

    # Convert list of tuples to dict for easier access
    params_dict = dict(serialized_params)

    assert "efun_query_start_date" in params_dict
    assert "efun_query_end_date" in params_dict
    assert "start_timestamp" in params_dict
    assert "end_timestamp" in params_dict

    start_date_param = params_dict["efun_query_start_date"]
    assert start_date_param["__class"] == "airflow.sdk.definitions.param.Param"
    start_date_value = start_date_param["default"]
    assert isinstance(start_date_value, str)
    assert "-" in start_date_value

    end_date_param = params_dict["efun_query_end_date"]
    end_date_value = end_date_param["default"]
    assert isinstance(end_date_value, str)
    assert "-" in end_date_value

    start_timestamp_param = params_dict["start_timestamp"]
    start_timestamp_value = start_timestamp_param["default"]
    assert isinstance(start_timestamp_value, int)

    end_timestamp_param = params_dict["end_timestamp"]
    end_timestamp_value = end_timestamp_param["default"]
    assert isinstance(end_timestamp_value, int)


if __name__ == "__main__":
    test_dag_validation_param_strftime_success()
    test_dag_validation_param_strftime_serialization()
