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

from datetime import datetime, timezone
from unittest import mock

import pytest

from airflow.providers.openlineage.conf import namespace
from airflow.providers.openlineage.plugins.macros import (
    lineage_job_name,
    lineage_job_namespace,
    lineage_parent_id,
    lineage_root_job_name,
    lineage_root_job_namespace,
    lineage_root_run_id,
    lineage_run_id,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

_DAG_NAMESPACE = namespace()

if AIRFLOW_V_3_0_PLUS:
    LOGICAL_DATE_KEY = "logical_date"
else:
    LOGICAL_DATE_KEY = "execution_date"


def test_lineage_job_namespace():
    assert lineage_job_namespace() == _DAG_NAMESPACE


def test_lineage_job_name():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        **{LOGICAL_DATE_KEY: datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)},
    )
    assert lineage_job_name(task_instance) == "dag_id.task_id"


def test_lineage_run_id():
    date = datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)
    dag_run = mock.MagicMock(run_id="run_id")
    dag_run.logical_date = date
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=dag_run,
        logical_date=date,
        try_number=1,
    )

    call_result1 = lineage_run_id(task_instance)
    call_result2 = lineage_run_id(task_instance)

    # random part value does not matter, it just has to be the same for the same TaskInstance
    assert call_result1 == call_result2
    # execution_date is used as most significant bits of UUID
    assert call_result1.startswith("016f5e9e-c4c8-")


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
def test_lineage_run_after_airflow_3():
    dag_run = mock.MagicMock(run_id="run_id")
    dag_run.run_after = datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)
    dag_run.logical_date = None
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=dag_run,
        try_number=1,
    )

    call_result1 = lineage_run_id(task_instance)
    call_result2 = lineage_run_id(task_instance)

    # random part value does not matter, it just has to be the same for the same TaskInstance
    assert call_result1 == call_result2
    # execution_date is used as most significant bits of UUID
    assert call_result1.startswith("016f5e9e-c4c8-")


@mock.patch("airflow.providers.openlineage.plugins.macros.lineage_run_id")
def test_lineage_parent_id(mock_run_id):
    mock_run_id.return_value = "run_id"
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        **{LOGICAL_DATE_KEY: datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)},
    )
    actual = lineage_parent_id(task_instance)
    expected = f"{_DAG_NAMESPACE}/dag_id.task_id/run_id"
    assert actual == expected


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
def test_lineage_root_run_id_with_runtime_task_instance(create_runtime_ti):
    """Test lineage_root_run_id with real RuntimeTaskInstance object doesn't throw AttributeError."""
    from airflow.providers.common.compat.sdk import BaseOperator

    task = BaseOperator(task_id="test_task")

    runtime_ti = create_runtime_ti(
        task=task,
        dag_id="test_dag",
        run_id="test_run_id",
    )

    # Explicitly test that the function doesn't throw an AttributeError
    # This was the original issue: AttributeError: 'RuntimeTaskInstance' object has no attribute 'dag_run'
    try:
        assert lineage_root_run_id(runtime_ti) is not None
    except AttributeError as e:
        pytest.fail(f"lineage_root_run_id should not throw AttributeError with RuntimeTaskInstance: {e}")


@pytest.mark.parametrize(
    "conf",
    (
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "rootParentJobNamespace": "rootns",
                "rootParentJobName": "rootjob",
            }
        },
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "rootParentJobNamespace": "rootns",
                "rootParentJobName": "rootjob",
                "parentRunId": "33333333-3333-3333-3333-333333333333",
                "parentJobNamespace": "parentns",
                "parentJobName": "parentjob",
            }
        },
    ),
)
@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
def test_lineage_root_macros_use_root_from_conf_af3(create_runtime_ti, conf):
    from airflow.providers.common.compat.sdk import BaseOperator

    task = BaseOperator(task_id="test_task")

    runtime_ti = create_runtime_ti(
        task=task,
        dag_id="test_dag",
        run_id="test_run_id",
        conf=conf,
    )

    root_run_id = lineage_root_run_id(runtime_ti)
    root_job_name = lineage_root_job_name(runtime_ti)
    root_job_namespace = lineage_root_job_namespace(runtime_ti)
    assert root_run_id == "22222222-2222-2222-2222-222222222222"
    assert root_job_name == "rootjob"
    assert root_job_namespace == "rootns"


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
def test_lineage_root_macros_use_parent_from_conf_when_root_missing_af3(create_runtime_ti):
    from airflow.providers.common.compat.sdk import BaseOperator

    task = BaseOperator(task_id="test_task")

    runtime_ti = create_runtime_ti(
        task=task,
        dag_id="test_dag",
        run_id="test_run_id",
        conf={
            "openlineage": {
                "parentRunId": "33333333-3333-3333-3333-333333333333",
                "parentJobNamespace": "parentns",
                "parentJobName": "parentjob",
            }
        },
    )

    root_run_id = lineage_root_run_id(runtime_ti)
    root_job_name = lineage_root_job_name(runtime_ti)
    root_job_namespace = lineage_root_job_namespace(runtime_ti)
    assert root_run_id == "33333333-3333-3333-3333-333333333333"
    assert root_job_name == "parentjob"
    assert root_job_namespace == "parentns"


@pytest.mark.parametrize(
    "conf",
    (
        {},
        None,
        {"some": "other"},
        {"openlineage": {}},
        {"openlineage": "some"},
        {"openlineage": {"rootParentRunId": "22222222-2222-2222-2222-222222222222"}},
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "rootParentJobName": "rootjob",
            }
        },
        {
            "openlineage": {
                "parentRunId": "33333333-3333-3333-3333-333333333333",
            }
        },
        {
            "openlineage": {
                "parentRunId": "33333333-3333-3333-3333-333333333333",
                "parentJobName": "parentjob",
            }
        },
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "parentRunId": "33333333-3333-3333-3333-333333333333",
            }
        },
    ),
)
@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
def test_lineage_root_macros_use_dagrun_info_when_missing_or_invalid_conf_af3(create_runtime_ti, conf):
    from airflow.providers.common.compat.sdk import BaseOperator

    task = BaseOperator(task_id="test_task")

    runtime_ti = create_runtime_ti(
        task=task,
        dag_id="test_dag",
        run_id="test_run_id",
        conf=conf,
    )

    root_run_id = lineage_root_run_id(runtime_ti)
    root_job_name = lineage_root_job_name(runtime_ti)
    root_job_namespace = lineage_root_job_namespace(runtime_ti)
    assert root_run_id == "01937fbb-4680-70b3-b49b-1de6b041527a"
    assert root_job_name == "test_dag"
    assert root_job_namespace == _DAG_NAMESPACE


@pytest.mark.parametrize(
    "conf",
    (
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "rootParentJobNamespace": "rootns",
                "rootParentJobName": "rootjob",
            }
        },
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "rootParentJobNamespace": "rootns",
                "rootParentJobName": "rootjob",
                "parentRunId": "33333333-3333-3333-3333-333333333333",
                "parentJobNamespace": "parentns",
                "parentJobName": "parentjob",
            }
        },
    ),
)
@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 2")
def test_lineage_root_macros_use_root_from_conf_af2(conf):
    task_instance = mock.MagicMock(dag_run=mock.MagicMock(conf=conf))

    root_run_id = lineage_root_run_id(task_instance)
    root_job_name = lineage_root_job_name(task_instance)
    root_job_namespace = lineage_root_job_namespace(task_instance)
    assert root_run_id == "22222222-2222-2222-2222-222222222222"
    assert root_job_name == "rootjob"
    assert root_job_namespace == "rootns"


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 2")
def test_lineage_root_macros_use_parent_from_conf_when_root_missing_af2():
    conf = {
        "openlineage": {
            "parentRunId": "33333333-3333-3333-3333-333333333333",
            "parentJobNamespace": "parentns",
            "parentJobName": "parentjob",
        }
    }
    task_instance = mock.MagicMock(dag_run=mock.MagicMock(conf=conf))

    root_run_id = lineage_root_run_id(task_instance)
    root_job_name = lineage_root_job_name(task_instance)
    root_job_namespace = lineage_root_job_namespace(task_instance)
    assert root_run_id == "33333333-3333-3333-3333-333333333333"
    assert root_job_name == "parentjob"
    assert root_job_namespace == "parentns"


@pytest.mark.parametrize(
    "conf",
    (
        {},
        None,
        {"some": "other"},
        {"openlineage": {}},
        {"openlineage": "some"},
        {"openlineage": {"rootParentRunId": "22222222-2222-2222-2222-222222222222"}},
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "rootParentJobName": "rootjob",
            }
        },
        {
            "openlineage": {
                "parentRunId": "33333333-3333-3333-3333-333333333333",
            }
        },
        {
            "openlineage": {
                "parentRunId": "33333333-3333-3333-3333-333333333333",
                "parentJobName": "parentjob",
            }
        },
        {
            "openlineage": {
                "rootParentRunId": "22222222-2222-2222-2222-222222222222",
                "parentRunId": "33333333-3333-3333-3333-333333333333",
            }
        },
    ),
)
@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 2")
def test_lineage_root_macros_use_dagrun_info_when_missing_or_invalid_conf_af2(conf):
    date = datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc)
    conf = {}
    dag_run = mock.MagicMock(run_id="run_id", conf=conf)
    dag_run.logical_date = date
    dag_run.clear_number = 1
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=dag_run,
        logical_date=date,
        try_number=1,
    )

    root_run_id = lineage_root_run_id(task_instance)
    root_job_name = lineage_root_job_name(task_instance)
    root_job_namespace = lineage_root_job_namespace(task_instance)
    assert root_run_id == "016f5e9e-c4c8-7c30-9eda-d9c646d633ea"
    assert root_job_name == "dag_id"
    assert root_job_namespace == _DAG_NAMESPACE
