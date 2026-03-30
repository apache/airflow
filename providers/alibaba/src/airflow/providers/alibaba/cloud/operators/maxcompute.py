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
"""This module contains Alibaba Cloud MaxCompute operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.alibaba.cloud.hooks.maxcompute import MaxComputeHook
from airflow.providers.alibaba.cloud.links.maxcompute import MaxComputeLogViewLink
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from odps.models import Instance

    from airflow.sdk import Context


class MaxComputeSQLOperator(BaseOperator):
    """
    Executes an SQL statement in MaxCompute.

    Waits for the SQL task instance to complete and returns instance id.

    :param sql: The SQL statement to run.
    :param project: The project ID to use.
    :param endpoint: The endpoint to use.
    :param priority: The priority of the SQL statement ranges from 0 to 9,
        applicable to projects with the job priority feature enabled.
        Takes precedence over the  `odps.instance.priority` setting from `hints`.
        Defaults to 9.
        See https://www.alibabacloud.com/help/en/maxcompute/user-guide/job-priority
        for details.
    :param running_cluster: The cluster to run the SQL statement on.
    :param hints: Hints for setting runtime parameters. See
        https://pyodps.readthedocs.io/en/latest/base-sql.html#id4 and
        https://www.alibabacloud.com/help/en/maxcompute/user-guide/flag-parameters
        for details.
    :param aliases: Aliases for the SQL statement.
    :param default_schema: The default schema to use.
    :param quota_name: The quota name to use.
        Defaults to project default quota if not specified.
    :param maxcompute_conn_id: The connection ID to use. Defaults to
        `maxcompute_default` if not specified.
    :param cancel_on_kill: Flag which indicates whether to stop running instance
        or not when task is killed. Default is True.
    """

    template_fields: Sequence[str] = (
        "sql",
        "project",
        "endpoint",
        "priority",
        "running_cluster",
        "hints",
        "aliases",
        "default_schema",
        "quota_name",
        "maxcompute_conn_id",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    operator_extra_links = (MaxComputeLogViewLink(),)

    def __init__(
        self,
        *,
        sql: str,
        project: str | None = None,
        endpoint: str | None = None,
        priority: int | None = None,
        running_cluster: str | None = None,
        hints: dict[str, str] | None = None,
        aliases: dict[str, str] | None = None,
        default_schema: str | None = None,
        quota_name: str | None = None,
        maxcompute_conn_id: str = "maxcompute_default",
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.project = project
        self.endpoint = endpoint
        self.priority = priority
        self.running_cluster = running_cluster
        self.hints = hints
        self.aliases = aliases
        self.default_schema = default_schema
        self.quota_name = quota_name
        self.maxcompute_conn_id = maxcompute_conn_id
        self.cancel_on_kill = cancel_on_kill
        self.hook: MaxComputeHook | None = None
        self.instance: Instance | None = None

    def execute(self, context: Context) -> str:
        self.hook = MaxComputeHook(maxcompute_conn_id=self.maxcompute_conn_id)

        self.instance = self.hook.run_sql(
            sql=self.sql,
            project=self.project,
            endpoint=self.endpoint,
            priority=self.priority,
            running_cluster=self.running_cluster,
            hints=self.hints,
            aliases=self.aliases,
            default_schema=self.default_schema,
            quota_name=self.quota_name,
        )

        MaxComputeLogViewLink.persist(context=context, log_view_url=self.instance.get_logview_address())

        self.instance.wait_for_success()

        return self.instance.id

    def on_kill(self) -> None:
        instance_id = self.instance.id if self.instance else None

        if instance_id and self.hook and self.cancel_on_kill:
            self.hook.stop_instance(
                instance_id=instance_id,
                project=self.project,
                endpoint=self.endpoint,
            )
        else:
            self.log.info("Skipping to stop instance: %s:%s.%s", self.project, self.endpoint, instance_id)
