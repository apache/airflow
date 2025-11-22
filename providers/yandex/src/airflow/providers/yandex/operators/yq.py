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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.yandex.hooks.yq import YQHook
from airflow.providers.yandex.links.yq import YQLink

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class YQExecuteQueryOperator(BaseOperator):
    """
    Executes sql code using Yandex Query service.

    :param sql: the SQL code to be executed as a single string
    :param name: name of the query in YandexQuery
    :param folder_id: cloud folder id where to create query
    :param yandex_conn_id: Airflow connection ID to get parameters from
    """

    operator_extra_links = (YQLink(),)
    template_fields: Sequence[str] = ("sql",)
    template_fields_renderers = {"sql": "sql"}
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        name: str | None = None,
        folder_id: str | None = None,
        yandex_conn_id: str | None = None,
        public_ssh_key: str | None = None,
        service_account_id: str | None = None,
        sql: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.folder_id = folder_id
        self.yandex_conn_id = yandex_conn_id
        self.public_ssh_key = public_ssh_key
        self.service_account_id = service_account_id
        self.sql = sql

        self.query_id: str | None = None

    @cached_property
    def hook(self) -> YQHook:
        """Get valid hook."""
        return YQHook(
            yandex_conn_id=self.yandex_conn_id,
            default_folder_id=self.folder_id,
            default_public_ssh_key=self.public_ssh_key,
            default_service_account_id=self.service_account_id,
        )

    def execute(self, context: Context) -> Any:
        self.query_id = self.hook.create_query(query_text=self.sql, name=self.name)

        # pass to YQLink
        web_link = self.hook.compose_query_web_link(self.query_id)
        YQLink.persist(context, web_link)

        results = self.hook.wait_results(self.query_id)
        # forget query to avoid 'stop_query' in on_kill
        self.query_id = None
        return results

    def on_kill(self) -> None:
        if self.hook is not None and self.query_id is not None:
            self.hook.stop_query(self.query_id)
            self.hook.close()
