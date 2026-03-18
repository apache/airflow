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

from datetime import timedelta
from typing import Any

import yandexcloud.auth as yc_auth
from yandex_query_client import YQHttpClient, YQHttpClientConfig

from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from airflow.providers.yandex.utils.user_agent import provider_user_agent


class YQHook(YandexCloudBaseHook):
    """A hook for Yandex Query."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        config = YQHttpClientConfig(
            token=self._get_iam_token(), project=self.default_folder_id, user_agent=provider_user_agent()
        )

        self.client: YQHttpClient = YQHttpClient(config=config)

    def close(self):
        """Release all resources."""
        self.client.close()

    def create_query(self, query_text: str | None, name: str | None = None) -> str:
        """
        Create and run query.

        :param query_text: SQL text.
        :param name: name for the query
        """
        return self.client.create_query(
            name=name,
            query_text=query_text,
        )

    def wait_results(self, query_id: str, execution_timeout: timedelta = timedelta(minutes=30)) -> Any:
        """
        Wait for query complete and get results.

        :param query_id: ID of query.
        :param execution_timeout: how long to wait for the query to complete.
        """
        result_set_count = self.client.wait_query_to_succeed(
            query_id, execution_timeout=execution_timeout, stop_on_timeout=True
        )

        return self.client.get_query_all_result_sets(query_id=query_id, result_set_count=result_set_count)

    def stop_query(self, query_id: str) -> None:
        """
        Stop the query.

        :param query_id: ID of the query.
        """
        self.client.stop_query(query_id)

    def get_query(self, query_id: str) -> Any:
        """
        Get query info.

        :param query_id: ID of the query.
        """
        return self.client.get_query(query_id)

    def get_query_status(self, query_id: str) -> str:
        """
        Get status of the query.

        :param query_id: ID of query.
        """
        return self.client.get_query_status(query_id)

    def compose_query_web_link(self, query_id: str):
        """
        Compose web link to query in Yandex Query UI.

        :param query_id: ID of query.
        """
        return self.client.compose_query_web_link(query_id)

    def _get_iam_token(self) -> str:
        if "token" in self.credentials:
            return self.credentials["token"]

        return yc_auth.get_auth_token(service_account_key=self.credentials.get("service_account_key"))
