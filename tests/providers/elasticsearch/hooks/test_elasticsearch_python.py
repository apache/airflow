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

from unittest import mock

from elasticsearch import Elasticsearch

from airflow.providers.elasticsearch.hooks.elasticsearch_python import ElasticsearchPythonHook


class MockElasticsearch:
    def __init__(self, data: dict):
        self.data = data

    def search(self, **kwargs):
        return self.data


class TestElasticsearchPythonHook:
    def setup(self):
        self.elasticsearch_hook = ElasticsearchPythonHook(hosts=["http://localhost:9200"])

    def test_client(self):
        es_connection = self.elasticsearch_hook.get_conn()
        assert isinstance(es_connection, Elasticsearch)

    @mock.patch("airflow.providers.elasticsearch.hooks.elasticsearch_python.ElasticsearchPythonHook.get_conn")
    def test_search(self, elastic_mock):
        es_data = {"hits": "test_hit"}
        es_client = MockElasticsearch(es_data)
        elastic_mock.return_value = es_client
        print(MockElasticsearch.search)

        query = {"test_query": "test_filter"}

        result = self.elasticsearch_hook.search(index="test_index", query=query)

        assert result == es_data['hits']
