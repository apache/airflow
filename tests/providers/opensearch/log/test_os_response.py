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

from typing import Any

import pytest

from airflow.providers.opensearch.log.os_response import (
    AttributeList,
    Hit,
    HitMeta,
    OpensearchResponse,
)
from airflow.providers.opensearch.log.os_task_handler import OpensearchTaskHandler

pytestmark = pytest.mark.db_test


class TestHitAndHitMetaAndOpenSearchResponse:
    OS_DOCUMENT: dict[str, Any] = {
        "_shards": {"failed": 0, "skipped": 0, "successful": 7, "total": 7},
        "hits": {
            "hits": [
                {
                    "_id": "jdeZT4kBjAZqZnexVUxk",
                    "_index": ".ds-filebeat-8.8.2-2023.07.09-000001",
                    "_score": 2.482621,
                    "_source": {
                        "@timestamp": "2023-07-13T14:13:15.140Z",
                        "asctime": "2023-07-09T07:47:43.907+0000",
                        "container": {"id": "airflow"},
                        "dag_id": "example_bash_operator",
                        "ecs": {"version": "8.0.0"},
                        "execution_date": "2023_07_09T07_47_32_000000",
                        "filename": "taskinstance.py",
                        "input": {"type": "log"},
                        "levelname": "INFO",
                        "lineno": 1144,
                        "log": {
                            "file": {
                                "path": "/opt/airflow/Documents/GitHub/airflow/logs/"
                                "dag_id=example_bash_operator'"
                                "/run_id=owen_run_run/task_id=run_after_loop/attempt=1.log"
                            },
                            "offset": 0,
                        },
                        "log.offset": 1688888863907337472,
                        "log_id": "example_bash_operator-run_after_loop-owen_run_run--1-1",
                        "message": "Dependencies all met for "
                        "dep_context=non-requeueable deps "
                        "ti=<TaskInstance: "
                        "example_bash_operator.run_after_loop "
                        "owen_run_run [queued]>",
                        "task_id": "run_after_loop",
                        "try_number": "1",
                    },
                    "_type": "_doc",
                }
            ]
        },
    }
    HIT_DOCUMENT = OS_DOCUMENT["hits"]["hits"][0]

    def test_hit_initialization_and_to_dict(self):
        hit = Hit(self.HIT_DOCUMENT)

        assert hit.asctime == "2023-07-09T07:47:43.907+0000"
        assert hit.dag_id == "example_bash_operator"
        assert hit.lineno == 1144
        assert (
            hit.log.file.path
            == "/opt/airflow/Documents/GitHub/airflow/logs/dag_id=example_bash_operator'/run_id=owen_run_run/task_id=run_after_loop/attempt=1.log"
        )

        # Test meta attribute
        assert isinstance(hit.meta, HitMeta)
        assert hit.to_dict() == self.HIT_DOCUMENT["_source"]

    def test_hitmeta_initialization_and_to_dict(self):
        hitmeta = HitMeta(self.HIT_DOCUMENT)

        assert hitmeta.id == "jdeZT4kBjAZqZnexVUxk"
        assert hitmeta.index == ".ds-filebeat-8.8.2-2023.07.09-000001"
        assert hitmeta.score == 2.482621
        assert hitmeta.doc_type == "_doc"

        expected_dict = {
            k[1:] if k.startswith("_") else k: v for (k, v) in self.HIT_DOCUMENT.items() if k != "_source"
        }
        expected_dict["doc_type"] = expected_dict.pop("type")
        assert hitmeta.to_dict() == expected_dict

    def test_opensearchresponse_initialization_and_hits_and_bool(self):
        task_handler = OpensearchTaskHandler(
            base_log_folder="local/log/location",
            end_of_log_mark="end_of_log\n",
            write_stdout=False,
            host="localhost",
            port=9200,
            username="dummy",
            password="dummy",
            json_format=False,
            json_fields="asctime,filename,lineno,levelname,message,exc_text",
        )
        response = OpensearchResponse(task_handler, self.OS_DOCUMENT)

        assert response._d_ == self.OS_DOCUMENT
        assert isinstance(response.hits, AttributeList)

        for hit in response.hits:
            assert isinstance(hit, Hit)
            assert isinstance(hit.meta, HitMeta)

        assert response.hits[0].asctime == "2023-07-09T07:47:43.907+0000"
        assert response.hits[0].levelname == "INFO"

        assert bool(response) is True
