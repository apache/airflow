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
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch

from airflow.hooks.base import BaseHook


class ElasticsearchPythonHook(BaseHook):
    """
    Interacts with Elasticsearch. This hook uses the official Elasticsearch Python Client.

    :param hosts: list: A list of a single or many Elasticsearch instances. Example: ["http://localhost:9200"]
    :param es_conn_args: dict: Additional arguments you might need to enter to connect to Elasticsearch.
                                Example: {"ca_cert":"/path/to/http_ca.crt", "basic_auth": "(user, pass)"}
    """

    def __init__(self, hosts: List[Any], es_conn_args: Optional[dict] = None):
        super().__init__()
        self.hosts = hosts
        self.es_conn_args = es_conn_args if es_conn_args else {}

    def get_conn(self) -> Elasticsearch:
        """Returns the Elasticsearch client"""
        client = Elasticsearch(self.hosts, **self.es_conn_args)

        return client

    def search(self, query: Dict[Any, Any], index: str = "_all") -> dict:
        """
        Returns results matching a query using Elasticsearch DSL

        :param index: str: The index you want to query
        :param query: dict: The query you want to run

        :returns: dict: The response 'hits' object from Elasticsearch
        """
        es_client = self.get_conn()
        result = es_client.search(index=index, body=query)
        return result['hits']
