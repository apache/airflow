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
from requests.packages.urllib3.util.retry import Retry

from datetime import timedelta, datetime
from enum import Enum
import logging
import requests
import time
from typing import Any
import jwt

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.

import http.client
http.client.HTTPConnection.debuglevel = 1

# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from airflow.exceptions import AirflowException
from airflow.providers.yandex.utils.user_agent import provider_user_agent

from .http_client import YQHttpClientConfig, YQHttpClient

class QueryType(Enum):
    ANALYTICS = 1
    STREAMING = 2

class YQHook(YandexCloudBaseHook):
    """
    A hook for Yandex Query
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        config = YQHttpClientConfig(
            token=self._get_iam_token(),
            project=self.default_folder_id,
            user_agent=provider_user_agent()
        )

        self.client: YQHttpClient = YQHttpClient(config=config)

    def close(self):
        self.client.close()

    def create_query(self, query_text: str|None, name: str|None=None, description: str | None = None, query_type: QueryType = QueryType.ANALYTICS) -> str:
        type = "ANALYTICS" if query_type == QueryType.ANALYTICS else "STREAMING"
        
        return self.client.create_query(
            name=name,
            type=type,
            query_text=query_text,
            description=description
        )

    def stop_query(self, query_id: str) -> None:
        self.client.stop_query(query_id)

    def get_query(self, query_id: str) -> Any:
        return self.client.get_query(query_id)

    def get_query_status(self, query_id: str) -> str:
        return self.client.get_query_status(query_id)

    def wait_results(self, query_id: str) -> Any:
        result_set_count = self.client.wait_query_to_succeed(
            query_id,
            execution_timeout=timedelta(minutes=30),
            stop_on_timeout=True
        )

        return self.client.get_query_all_result_sets(query_id=query_id, result_set_count=result_set_count)

    def _get_iam_token(self) -> str:
        if "token" in self.credentials:
            return self.credentials["token"]
        if "service_account_key" in self.credentials:
            return YQHook._resolve_service_account_key(self.credentials["service_account_key"])
        raise AirflowException(f"Unknown credentials type, available keys {self.credentials.keys()}")

    def compose_query_web_link(self, query_id:str):
        return self.client.compose_query_web_link(query_id)
    
    @staticmethod
    def _resolve_service_account_key(sa_info) -> str:
        with YQHook.create_session() as session:
            api = 'https://iam.api.cloud.yandex.net/iam/v1/tokens'
            now = int(time.time())
            payload = {
                'aud': api,
                'iss': sa_info["service_account_id"],
                'iat': now,
                'exp': now + 360
            }

            encoded_token = jwt.encode(
                payload,
                sa_info["private_key"],
                algorithm='PS256',
                headers={'kid': sa_info["id"]}
            )

            data = {"jwt": encoded_token}
            iam_response = session.post(api, json=data)
            iam_response.raise_for_status()

            return iam_response.json()["iamToken"]

    @staticmethod
    def create_session() -> requests.Session:
        session = requests.Session()
        session.verify = False
        session.timeout = 20
        retry = Retry(
            backoff_factor=0.3,
            total=10
        )
        session.mount(
            'http://',
            requests.adapters.HTTPAdapter(max_retries=retry)
        )
        session.mount(
            'https://',
            requests.adapters.HTTPAdapter(max_retries=retry)
        )

        return session
    