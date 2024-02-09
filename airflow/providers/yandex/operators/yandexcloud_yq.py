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

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence
from datetime import timedelta
from airflow.configuration import conf
import pandas as pd


from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.yandex.hooks.yandexcloud_yq import YQHook, QueryType
from airflow.providers.yandex.triggers.yandexcloud_yq import YQQueryStatusTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

class YQExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    Executes sql code using a specific Trino query Engine.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
    """

    template_fields: Sequence[str] = ("sql",)
    template_fields_renderers = {"sql": "sql"}
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        type: QueryType = QueryType.ANALYTICS,
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
        self.type = type
        self.deferrable = deferrable
        self.folder_id = folder_id
        self.connection_id = connection_id
        self.public_ssh_key = public_ssh_key
        self.service_account_id = service_account_id

        self.hook: YQHook | None = None

    def execute(self, context: Context) -> None:
        self.hook = YQHook(
            yandex_conn_id=self.connection_id,
            default_folder_id=self.folder_id,
            default_public_ssh_key=self.public_ssh_key,
            default_service_account_id=self.service_account_id
        )

        self.hook.start_execute_query(self.type, self.sql, self.name, self.description)

        # value = [1]
        # # Deprecated
        # context["task_instance"].xcom_push(key="query_result", value=value)
        # return value

        self.log.info(f"deffered is allowed [{self.deferrable}]")

        if self.deferrable:
        # if True:
            self.defer(
                trigger=YQQueryStatusTrigger(
                    poll_interval=timedelta(seconds=2).seconds,
                    query_id=self.hook.query_id,
                    connection_id=self.connection_id,
                    folder_id=self.folder_id,
                    public_ssh_key=self.public_ssh_key,
                    service_account_id=self.service_account_id
                ),
                method_name="execute_complete",
            )
        else:
            self.hook.wait_for_query_to_complete(self.execution_timeout)
            return self.hook.get_query_result(self.hook.query_id)

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> None:
        if "status" in event and event["status"]!="COMPLETED":
            msg = None
            if 'message' in event:
                msg = f"{event['status']}: {event['message']}"
            else:
                msg = event["status"]
            raise AirflowException(msg)
        else:
            query_id = event["query_id"]

            hook = YQHook(  connection_id=event["connection_id"],
                            default_folder_id=event["folder_id"],
                            default_public_ssh_key=event["public_ssh_key"],
                            default_service_account_id=event["service_account_id"])

            result = hook.get_query_result(query_id)
            self.log.info("%s completed successfully.", self.task_id)
            return result

    def on_kill():
        if self.hook is not None:
            self.hook.stop_current_query()

    @staticmethod
    def to_dataframe(data):
        column_names = [column["name"] for column in data["columns"]]
        return pd.DataFrame(data["rows"], columns= column_names)
