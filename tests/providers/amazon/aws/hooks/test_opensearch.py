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

import pytest
from moto import mock_opensearch
import boto3

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.opensearch import OpenSearchHook
from airflow.utils import db


class TestOpenSearchHook:
    @mock_opensearch
    def create_domain(self):
        client = boto3.client("opensearch")
        response = client.create_domain(DomainName=f"test-opensearch-cluster",
                                        EngineVersion="2.7",
                                        ClusterConfig={
                                            "InstanceType": "t3.small.search",
                                            "InstanceCount": 1,
                                            "DedicatedMasterEnabled": False,
                                            "ZoneAwarenessEnabled": False,
                                         },)
        return response["endpoint"]

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="opensearch_default",
                conn_type="opensearch",
                host=self.create_domain(),
                login="MyAWSSecretID",
                password="MyAccessKey",
                extra={
                    "region_name": "us-east-1"
                }
            )
        )

    @pytest.fixture()
    def mock_search(self, monkeypatch):
        def mock_return(index_name: str):
            return {"status": "test"}

        monkeypatch.setattr(OpenSearchHook, "search", mock_return)

    def test_hook_search(self, mock_search):
        hook = OpenSearchHook(open_search_conn_id="opensearch_default",
                              log_query=True)

        result = hook.search(
            index_name="testIndex",
            query={"size": 1, "query": {"multi_match": {"query": "test", "fields": ["testField"]}}},
        )

        assert result
