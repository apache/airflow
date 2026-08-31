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

from airflow.providers.common.search.log.shared_response import SearchResponse
from airflow.providers.elasticsearch.log.es_response import AttributeList, ElasticSearchResponse, Hit, HitMeta
from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler


class TestElasticSearchResponse:
    """
    Behavior of AttributeDict/AttributeList/Hit/HitMeta/SearchResponse is exercised by
    apache-airflow-providers-common-search; this only checks the elasticsearch alias
    wiring against a real ElasticsearchTaskHandler.
    """

    ES_DOCUMENT: dict[str, Any] = {
        "hits": {
            "hits": [
                {
                    "_id": "jdeZT4kBjAZqZnexVUxk",
                    "_source": {
                        "asctime": "2023-07-09T07:47:43.907+0000",
                        "levelname": "INFO",
                    },
                    "_type": "_doc",
                }
            ]
        },
    }

    def test_elastic_search_response_is_shared_search_response(self):
        assert ElasticSearchResponse is SearchResponse

    def test_elasticsearchresponse_initialization_and_hits_and_bool(self):
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

        assert bool(response) is True
