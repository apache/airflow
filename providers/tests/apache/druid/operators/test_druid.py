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

import json
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.apache.druid.hooks.druid import IngestionType
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.utils import timezone
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

JSON_INDEX_STR = """
    {
        "type": "{{ params.index_type }}",
        "datasource": "{{ params.datasource }}",
        "spec": {
            "dataSchema": {
                "granularitySpec": {
                    "intervals": ["{{ ds }}/{{ macros.ds_add(ds, 1) }}"]
                }
            }
        }
    }
"""

RENDERED_INDEX = {
    "type": "index_hadoop",
    "datasource": "datasource_prd",
    "spec": {"dataSchema": {"granularitySpec": {"intervals": ["2017-01-01/2017-01-02"]}}},
}


@pytest.mark.need_serialized_dag
@pytest.mark.db_test
def test_render_template(dag_maker):
    with dag_maker(
        "test_druid_render_template",
        schedule="@daily",
        default_args={"start_date": DEFAULT_DATE},
    ):
        operator = DruidOperator(
            task_id="spark_submit_job",
            json_index_file=JSON_INDEX_STR,
            params={"index_type": "index_hadoop", "datasource": "datasource_prd"},
        )

    dag_run = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
    dag_maker.session.add(dag_run.task_instances[0])
    dag_maker.session.commit()
    dag_run.task_instances[0].render_templates()
    assert RENDERED_INDEX == json.loads(operator.json_index_file)


@pytest.mark.need_serialized_dag
@pytest.mark.db_test
def test_render_template_from_file(tmp_path, dag_maker):
    json_index_file = tmp_path.joinpath("json_index.json")
    json_index_file.write_text(JSON_INDEX_STR)

    with dag_maker(
        "test_druid_render_template_from_file",
        schedule="@daily",
        template_searchpath=[str(tmp_path)],
        default_args={"start_date": DEFAULT_DATE},
    ):
        operator = DruidOperator(
            task_id="spark_submit_job",
            json_index_file=json_index_file.name,
            params={"index_type": "index_hadoop", "datasource": "datasource_prd"},
        )

    dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED).task_instances[
        0
    ].render_templates()
    assert RENDERED_INDEX == json.loads(operator.json_index_file)


def test_init_with_timeout_and_max_ingestion_time():
    operator = DruidOperator(
        task_id="spark_submit_job",
        json_index_file=JSON_INDEX_STR,
        timeout=60,
        max_ingestion_time=180,
        params={"index_type": "index_hadoop", "datasource": "datasource_prd"},
    )

    expected_values = {
        "task_id": "spark_submit_job",
        "timeout": 60,
        "max_ingestion_time": 180,
    }
    assert expected_values["task_id"] == operator.task_id
    assert expected_values["timeout"] == operator.timeout
    assert expected_values["max_ingestion_time"] == operator.max_ingestion_time


def test_init_defaults():
    operator = DruidOperator(
        task_id="spark_submit_job",
        json_index_file=JSON_INDEX_STR,
        params={"index_type": "index_hadoop", "datasource": "datasource_prd"},
    )
    expected_default_druid_ingest_conn_id = "druid_ingest_default"
    expected_default_timeout = 1
    expected_default_max_ingestion_time = None
    expected_default_ingestion_type = IngestionType.BATCH
    expected_default_verify_ssl = True
    assert expected_default_druid_ingest_conn_id == operator.conn_id
    assert expected_default_timeout == operator.timeout
    assert expected_default_max_ingestion_time == operator.max_ingestion_time
    assert expected_default_ingestion_type == operator.ingestion_type
    assert expected_default_verify_ssl == operator.verify_ssl


@patch("airflow.providers.apache.druid.operators.druid.DruidHook")
def test_execute_calls_druid_hook_with_the_right_parameters(mock_druid_hook):
    mock_druid_hook_instance = MagicMock()
    mock_druid_hook.return_value = mock_druid_hook_instance
    json_index_file = "sql.json"
    druid_ingest_conn_id = "druid_ingest_default"
    max_ingestion_time = 5
    timeout = 5
    verify_ssl = False
    operator = DruidOperator(
        task_id="spark_submit_job",
        json_index_file=json_index_file,
        druid_ingest_conn_id=druid_ingest_conn_id,
        timeout=timeout,
        ingestion_type=IngestionType.MSQ,
        max_ingestion_time=max_ingestion_time,
        verify_ssl=verify_ssl,
    )
    operator.execute(context={})
    mock_druid_hook.assert_called_once_with(
        druid_ingest_conn_id=druid_ingest_conn_id,
        timeout=timeout,
        max_ingestion_time=max_ingestion_time,
        verify_ssl=verify_ssl,
    )
    mock_druid_hook_instance.submit_indexing_job.assert_called_once_with(
        json_index_file, IngestionType.MSQ
    )
