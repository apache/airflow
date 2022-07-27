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

from datetime import datetime

from airflow import DAG
from airflow.api_connexion.schemas.dag_schema import (
    DAGCollection,
    DAGCollectionSchema,
    DAGDetailSchema,
    DAGSchema,
)
from airflow.models import DagModel, DagTag


def test_serialize_test_dag_schema(url_safe_serializer):
    dag_model = DagModel(
        dag_id="test_dag_id",
        root_dag_id="test_root_dag_id",
        is_paused=True,
        is_active=True,
        is_subdag=False,
        fileloc="/root/airflow/dags/my_dag.py",
        owners="airflow1,airflow2",
        description="The description",
        schedule_interval="5 4 * * *",
        tags=[DagTag(name="tag-1"), DagTag(name="tag-2")],
    )
    serialized_dag = DAGSchema().dump(dag_model)

    assert {
        "dag_id": "test_dag_id",
        "description": "The description",
        "fileloc": "/root/airflow/dags/my_dag.py",
        "file_token": url_safe_serializer.dumps("/root/airflow/dags/my_dag.py"),
        "is_paused": True,
        "is_active": True,
        "is_subdag": False,
        "owners": ["airflow1", "airflow2"],
        "root_dag_id": "test_root_dag_id",
        "schedule_interval": {"__type": "CronExpression", "value": "5 4 * * *"},
        "tags": [{"name": "tag-1"}, {"name": "tag-2"}],
        'next_dagrun': None,
        'has_task_concurrency_limits': True,
        'next_dagrun_data_interval_start': None,
        'next_dagrun_data_interval_end': None,
        'max_active_runs': 16,
        'next_dagrun_create_after': None,
        'last_expired': None,
        'max_active_tasks': 16,
        'last_pickled': None,
        'default_view': None,
        'last_parsed_time': None,
        'scheduler_lock': None,
        'timetable_description': None,
        'has_import_errors': None,
        'pickle_id': None,
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
                "description": None,
                "fileloc": "/tmp/a.py",
                "file_token": url_safe_serializer.dumps("/tmp/a.py"),
                "is_paused": None,
                "is_subdag": None,
                "is_active": None,
                "owners": [],
                "root_dag_id": None,
                "schedule_interval": None,
                "tags": [],
                'next_dagrun': None,
                'has_task_concurrency_limits': True,
                'next_dagrun_data_interval_start': None,
                'next_dagrun_data_interval_end': None,
                'max_active_runs': 16,
                'next_dagrun_create_after': None,
                'last_expired': None,
                'max_active_tasks': 16,
                'last_pickled': None,
                'default_view': None,
                'last_parsed_time': None,
                'scheduler_lock': None,
                'timetable_description': None,
                'has_import_errors': None,
                'pickle_id': None,
            },
            {
                "dag_id": "test_dag_id_b",
                "description": None,
                "fileloc": "/tmp/a.py",
                "file_token": url_safe_serializer.dumps("/tmp/a.py"),
                "is_active": None,
                "is_paused": None,
                "is_subdag": None,
                "owners": [],
                "root_dag_id": None,
                "schedule_interval": None,
                "tags": [],
                'next_dagrun': None,
                'has_task_concurrency_limits': True,
                'next_dagrun_data_interval_start': None,
                'next_dagrun_data_interval_end': None,
                'max_active_runs': 16,
                'next_dagrun_create_after': None,
                'last_expired': None,
                'max_active_tasks': 16,
                'last_pickled': None,
                'default_view': None,
                'last_parsed_time': None,
                'scheduler_lock': None,
                'timetable_description': None,
                'has_import_errors': None,
                'pickle_id': None,
            },
        ],
        "total_entries": 2,
    } == schema.dump(instance)


def test_serialize_test_dag_detail_schema(url_safe_serializer):
    dag = DAG(
        dag_id="test_dag",
        start_date=datetime(2020, 6, 19),
        doc_md="docs",
        orientation="LR",
        default_view="duration",
        params={"foo": 1},
        tags=['example1', 'example2'],
    )
    schema = DAGDetailSchema()

    expected = {
        'catchup': True,
        'concurrency': 16,
        'max_active_tasks': 16,
        'dag_id': 'test_dag',
        'dag_run_timeout': None,
        'default_view': 'duration',
        'description': None,
        'doc_md': 'docs',
        'fileloc': __file__,
        "file_token": url_safe_serializer.dumps(__file__),
        "is_active": None,
        'is_paused': None,
        'is_subdag': False,
        'orientation': 'LR',
        'owners': [],
        'params': {
            'foo': {
                '__class': 'airflow.models.param.Param',
                'value': 1,
                'description': None,
                'schema': {},
            }
        },
        'schedule_interval': {'__type': 'TimeDelta', 'days': 1, 'seconds': 0, 'microseconds': 0},
        'start_date': '2020-06-19T00:00:00+00:00',
        'tags': [{'name': "example1"}, {'name': "example2"}],
        'timezone': "Timezone('UTC')",
        'max_active_runs': 16,
        'pickle_id': None,
        "end_date": None,
        'is_paused_upon_creation': None,
        'render_template_as_native_obj': False,
    }
    obj = schema.dump(dag)
    expected.update({'last_parsed': obj['last_parsed']})
    assert obj == expected
