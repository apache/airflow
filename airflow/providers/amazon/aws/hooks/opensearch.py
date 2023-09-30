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
from typing import Any

from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

from airflow.exceptions import AirflowException
from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsOpenSearchHook(OpenSearchHook, AwsBaseHook):
    """
    This Hook provides a thin wrapper around the OpenSearch client.

    :param: open_search_conn_id: AWS Connection to use with Open Search
    :param: log_query: Whether to log the query used for Open Search
    """
    conn_name_attr = "aws_opensearch_conn_id"
    default_conn_name = "aws_opensearch_default"
    conn_type = "opensearch"
    hook_name = "AWS Open Search Hook"

    def __init__(self, *args: Any, open_search_conn_id: str, log_query: bool, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.region = self.conn.extra_dejson.get("region_name", self.region_name)

        self._credentials = self.get_credentials(self.region)
        self._auth = AWSV4SignerAuth(self._credentials, self.region, self.__SERVICE)

    def get_client(self) -> OpenSearch:
        """

        This function is intended for Operators that will take in arguments and use the high level
        OpenSearch client which allows using Python objects to perform searches.

        """
        self.client = OpenSearch(
            hosts=[{"host": self.conn.host, "port": self.conn.port}],
            http_auth=self._auth,
            use_ssl=self.use_ssl,
            verify_certs=self.verify_certs,
            connection_class=RequestsHttpConnection,
        )
        return self.client

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom UI field behaviour for Amazon Open Search Connection."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "host": "OpenSearch Cluster Endpoint",
                "login": "AWS Access Key ID",
                "password": "AWS Secret Access Key",
                "extra": "Open Search Configuration",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "use_ssl": True,
                        "verify_certs": True,
                        "region_name": "us-east-1"
                    },
                    indent=2,
                ),
            },
        }
