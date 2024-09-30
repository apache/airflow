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

from datetime import datetime, timedelta

import pendulum
import pytest

from airflow.api_connexion.schemas.dag_schema import (
    DAGCollection,
    DAGCollectionSchema,
    DAGDetailSchema,
    DAGSchema,
)
from airflow.assets import Asset
from airflow.models import DagModel, DagTag
from airflow.models.dag import DAG

UTC_JSON_REPR = "UTC" if pendulum.__version__.startswith("3") else "Timezone('UTC')"


def test_serialize_test_dag_schema(url_safe_serializer):
    dag_model = DagModel(
        dag_id="test_dag_id",
        is_paused=True,
        is_active=True,
        fileloc="/root/airflow/dags/my_dag.py",
        owners="airflow1,airflow2",
        description="The description",
        timetable_summary="5 4 * * *",
        tags=[DagTag(name="tag-1"), DagTag(name="tag-2")],
    )
    serialized_dag = DAGSchema().dump(dag_model)

    assert {
        "dag_id": "test_dag_id",
        "dag_display_name": "test_dag_id",
        "description": "The description",
        "fileloc": "/root/airflow/dags/my_dag.py",
        "file_token": url_safe_serializer.dumps("/root/airflow/dags/my_dag.py"),
        "is_paused": True,
        "is_active": True,
        "owners": ["airflow1", "airflow2"],
        "timetable_summary": "5 4 * * *",
        "tags": [{"name": "tag-1"}, {"name": "tag-2"}],
        "next_dagrun": None,
        "has_task_concurrency_limits": True,
        "next_dagrun_data_interval_start": None,
        "next_dagrun_data_interval_end": None,
        "max_active_runs": 16,
        "max_consecutive_failed_dag_runs": 0,
        "next_dagrun_create_after": None,
        "last_expired": None,
        "max_active_tasks": 16,
        "last_pickled": None,
        "default_view": None,
        "last_parsed_time": None,
        "scheduler_lock": None,
        "timetable_description": None,
        "has_import_errors": None,
        "pickle_id": None,
    } == serialized_dag


def test_serialize_test_dag_collection_schema(url_safe_serializer):
    dag_model_a = DagModel(dag_id="test_dag_id_a", fileloc="/tmp/a.py")
    dag_model_b = DagModel(dag_id="test_dag_id_b", fileloc="/tmp/a.py")
    schema = DAGCollectionSchema()
    instance = DAGCollection(dags=[dag_model_a, dag_model_b], total_entries=2)
    assert {
        "dags": [
            {
                "dag_id": "test_dag_id_a",
                "dag_display_name": "test_dag_id_a",
                "description": None,
                "fileloc": "/tmp/a.py",
                "file_token": url_safe_serializer.dumps("/tmp/a.py"),
                "is_paused": None,
                "is_active": None,
                "owners": [],
                "timetable_summary": None,
                "tags": [],
                "next_dagrun": None,
                "has_task_concurrency_limits": True,
                "next_dagrun_data_interval_start": None,
                "next_dagrun_data_interval_end": None,
                "max_active_runs": 16,
                "next_dagrun_create_after": None,
                "last_expired": None,
                "max_active_tasks": 16,
                "max_consecutive_failed_dag_runs": 0,
                "last_pickled": None,
                "default_view": None,
                "last_parsed_time": None,
                "scheduler_lock": None,
                "timetable_description": None,
                "has_import_errors": None,
                "pickle_id": None,
            },
            {
                "dag_id": "test_dag_id_b",
                "dag_display_name": "test_dag_id_b",
                "description": None,
                "fileloc": "/tmp/a.py",
                "file_token": url_safe_serializer.dumps("/tmp/a.py"),
                "is_active": None,
                "is_paused": None,
                "owners": [],
                "timetable_summary": None,
                "tags": [],
                "next_dagrun": None,
                "has_task_concurrency_limits": True,
                "next_dagrun_data_interval_start": None,
                "next_dagrun_data_interval_end": None,
                "max_active_runs": 16,
                "next_dagrun_create_after": None,
                "last_expired": None,
                "max_active_tasks": 16,
                "max_consecutive_failed_dag_runs": 0,
                "last_pickled": None,
                "default_view": None,
                "last_parsed_time": None,
                "scheduler_lock": None,
                "timetable_description": None,
                "has_import_errors": None,
                "pickle_id": None,
            },
        ],
        "total_entries": 2,
    } == schema.dump(instance)


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
def test_serialize_test_dag_detail_schema(url_safe_serializer):
    dag = DAG(
        dag_id="test_dag",
        schedule=timedelta(days=1),
        start_date=datetime(2020, 6, 19),
        doc_md="docs",
        orientation="LR",
        default_view="duration",
        params={"foo": 1},
        tags=["example1", "example2"],
    )
    schema = DAGDetailSchema()

    expected = {
        "catchup": True,
        "concurrency": 16,
        "max_active_tasks": 16,
        "dag_id": "test_dag",
        "dag_display_name": "test_dag",
        "dag_run_timeout": None,
        "default_view": "duration",
        "description": None,
        "doc_md": "docs",
        "fileloc": __file__,
        "file_token": url_safe_serializer.dumps(__file__),
        "is_active": None,
        "is_paused": None,
        "orientation": "LR",
        "owners": [],
        "params": {
            "foo": {
                "__class": "airflow.models.param.Param",
                "value": 1,
                "description": None,
                "schema": {},
            }
        },
        "start_date": "2020-06-19T00:00:00+00:00",
        "tags": sorted(
            [{"name": "example1"}, {"name": "example2"}],
            key=lambda val: val["name"],
        ),
        "template_searchpath": None,
        "timetable_summary": "1 day, 0:00:00",
        "timezone": UTC_JSON_REPR,
        "max_active_runs": 16,
        "max_consecutive_failed_dag_runs": 0,
        "pickle_id": None,
        "end_date": None,
        "is_paused_upon_creation": None,
        "render_template_as_native_obj": False,
    }
    obj = schema.dump(dag)
    expected.update({"last_parsed": obj["last_parsed"]})
    obj["tags"] = sorted(
        obj["tags"],
        key=lambda val: val["name"],
    )
    assert obj == expected


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
def test_serialize_test_dag_with_asset_schedule_detail_schema(url_safe_serializer):
    asset1 = Asset(uri="s3://bucket/obj1")
    asset2 = Asset(uri="s3://bucket/obj2")
    dag = DAG(
        dag_id="test_dag",
        start_date=datetime(2020, 6, 19),
        doc_md="docs",
        orientation="LR",
        default_view="duration",
        params={"foo": 1},
        schedule=asset1 & asset2,
        tags=["example1", "example2"],
    )
    schema = DAGDetailSchema()

    expected = {
        "catchup": True,
        "concurrency": 16,
        "max_active_tasks": 16,
        "dag_id": "test_dag",
        "dag_display_name": "test_dag",
        "dag_run_timeout": None,
        "default_view": "duration",
        "description": None,
        "doc_md": "docs",
        "fileloc": __file__,
        "file_token": url_safe_serializer.dumps(__file__),
        "is_active": None,
        "is_paused": None,
        "orientation": "LR",
        "owners": [],
        "params": {
            "foo": {
                "__class": "airflow.models.param.Param",
                "value": 1,
                "description": None,
                "schema": {},
            }
        },
        "start_date": "2020-06-19T00:00:00+00:00",
        "tags": sorted(
            [{"name": "example1"}, {"name": "example2"}],
            key=lambda val: val["name"],
        ),
        "template_searchpath": None,
        "timetable_summary": "Asset",
        "timezone": UTC_JSON_REPR,
        "max_active_runs": 16,
        "max_consecutive_failed_dag_runs": 0,
        "pickle_id": None,
        "end_date": None,
        "is_paused_upon_creation": None,
        "render_template_as_native_obj": False,
    }
    obj = schema.dump(dag)
    expected.update({"last_parsed": obj["last_parsed"]})
    obj["tags"] = sorted(
        obj["tags"],
        key=lambda val: val["name"],
    )
    assert obj == expected
