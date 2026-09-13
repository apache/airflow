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

# The wrapper behaviour itself is covered by ``shared/search/tests/search/test_response.py``.
# This module only covers what is specific to the provider: the historical import path and
# the wiring between ``ElasticsearchTaskHandler._get_result`` and the shared response class.

from __future__ import annotations

from typing import Any

from airflow.providers.elasticsearch._shared.search.response import SearchResponse
from airflow.providers.elasticsearch.log import es_response
from airflow.providers.elasticsearch.log.es_response import (
    AttributeList,
    ElasticSearchResponse,
    Hit,
    HitMeta,
)
from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler


def test_es_response_re_exports_shared_names():
    assert issubclass(ElasticSearchResponse, SearchResponse)
    for name in es_response.__all__:
        assert hasattr(es_response, name), f"{name} is declared in __all__ but not exported"


class TestElasticSearchResponse:
    ES_DOCUMENT: dict[str, Any] = {
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

    def test_hits_are_built_from_the_task_handler_results(self):
        task_handler = ElasticsearchTaskHandler(
            base_log_folder="local/log/location",
            end_of_log_mark="end_of_log\n",
            write_stdout=False,
            json_format=False,
            json_fields="asctime,filename,lineno,levelname,message,exc_text",
        )
        response = ElasticSearchResponse(task_handler, self.ES_DOCUMENT)

        assert response._d_ == self.ES_DOCUMENT
        assert isinstance(response.hits, AttributeList)

        for hit in response.hits:
            assert isinstance(hit, Hit)
            assert isinstance(hit.meta, HitMeta)

        assert response.hits[0].asctime == "2023-07-09T07:47:43.907+0000"
        assert response.hits[0].levelname == "INFO"
        assert response.hits[0].meta.id == "jdeZT4kBjAZqZnexVUxk"
        assert bool(response) is True
