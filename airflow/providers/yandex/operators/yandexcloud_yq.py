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

from typing import TYPE_CHECKING, Sequence, Any
from airflow.configuration import conf

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.yandex.hooks.yandexcloud_yq import YQHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

XCOM_WEBLINK_KEY="web_link"

class YQLink(BaseOperatorLink):
    name = "Yandex Query"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        return XCom.get_value(key=XCOM_WEBLINK_KEY, ti_key=ti_key) or "https://yq.cloud.yandex.ru"


class YQExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    Executes sql code using Yandex Query service.

    :param sql: the SQL code to be executed as a single string
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
        description: str | None = None,
        folder_id: str | None = None,
        connection_id: str | None = None,
        public_ssh_key: str | None = None,
        service_account_id: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.deferrable = deferrable
        self.folder_id = folder_id
        self.connection_id = connection_id
        self.public_ssh_key = public_ssh_key
        self.service_account_id = service_account_id

        self.hook: YQHook | None = None
        self.query_id: str | None

    def execute(self, context: Context) -> Any:
        self.hook = YQHook(
            yandex_conn_id=self.connection_id,
            default_folder_id=self.folder_id,
            default_public_ssh_key=self.public_ssh_key,
            default_service_account_id=self.service_account_id
        )

        self.query_id = self.hook.create_query(
            query_text=self.sql,
            name=self.name,
            description=self.description
        )

        # pass to YQLink
        web_link = self.hook.compose_query_web_link(self.query_id)
        context["ti"].xcom_push(key=XCOM_WEBLINK_KEY, value=web_link)

        results = self.hook.wait_results(self.query_id)
        # forget query to avoid 'stop_query' in on_kill
        self.query_id = None
        return results

    def on_kill(self) -> None:
        if self.hook is not None and self.query_id is not None:
            self.hook.stop_query(self.query_id)
