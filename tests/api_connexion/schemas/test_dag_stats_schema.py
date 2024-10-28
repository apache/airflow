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

from airflow.api_connexion.schemas.dag_stats_schema import (
    dag_stats_collection_schema,
    dag_stats_schema,
    dag_stats_state_schema,
)
from airflow.utils.state import DagRunState


class TestDagStatsStateSchema:
    def test_dag_stats_state_schema(self):
        payload = {
            "state": DagRunState.RUNNING,
            "count": 2,
        }
        serialized_data = dag_stats_state_schema.dump(payload)
        assert serialized_data == payload


class TestDagStatsSchema:
    def test_dag_stats_schema(self):
        payload = {
            "dag_id": "test_dag_id",
            "stats": [
                {
                    "state": DagRunState.QUEUED,
                    "count": 2,
                },
                {
                    "state": DagRunState.FAILED,
                    "count": 1,
                },
            ],
        }
        serialized_data = dag_stats_schema.dump(payload)
        assert serialized_data == payload


class TestDagStatsCollectionSchema:
    def test_dag_stats_collection_schema(self):
        payload = {
            "dags": [
                {
                    "dag_id": "test_dag_id",
                    "stats": [
                        {
                            "state": DagRunState.RUNNING,
                            "count": 2,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 1,
                        },
                    ],
                },
                {
                    "dag_id": "test_dag_id_2",
                    "stats": [
                        {
                            "state": DagRunState.RUNNING,
                            "count": 2,
                        }
                    ],
                },
                {"dag_id": "test_dag_id_3", "stats": []},
            ],
            "total_entries": 3,
        }
        serialized_data = dag_stats_collection_schema.dump(payload)
        assert serialized_data == payload
