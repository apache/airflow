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

import functools
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from odps import ODPS

from airflow.providers.alibaba.cloud.exceptions import MaxComputeConfigurationException
from airflow.providers.alibaba.cloud.hooks.base_alibaba import AlibabaBaseHook

if TYPE_CHECKING:
    from odps.models import Instance

RT = TypeVar("RT")


def fallback_to_default_project_endpoint(func: Callable[..., RT]) -> Callable[..., RT]:
    """
    Provide fallback for MaxCompute project and endpoint to be used as a decorator.

    If the project or endpoint is None it will be replaced with the project from the
    connection extra definition.

    :param func: function to wrap
    :return: result of the function call
    """

    @functools.wraps(func)
    def inner_wrapper(self, **kwargs) -> RT:
        required_args = ("project", "endpoint")
        for arg_name in required_args:
            # Use the value from kwargs if it is provided and value is not None, otherwise use the
            # value from the connection extra property.
            kwargs[arg_name] = getattr(self, arg_name) if kwargs.get(arg_name) is None else kwargs[arg_name]
            if not kwargs[arg_name]:
                raise MaxComputeConfigurationException(
                    f'"{arg_name}" must be passed either as '
                    "keyword parameter or as extra "
                    "in the MaxCompute connection definition. Both are not set!"
                )

        return func(self, **kwargs)

    return inner_wrapper


class MaxComputeHook(AlibabaBaseHook):
    """
    Interact with Alibaba MaxCompute (previously known as ODPS).

    :param maxcompute_conn_id: The connection ID to use when fetching connection info.
    """

    conn_name_attr = "maxcompute_conn_id"
    default_conn_name = "maxcompute_default"
    conn_type = "maxcompute"
    hook_name = "MaxCompute"

    def __init__(self, maxcompute_conn_id: str = "maxcompute_default", **kwargs) -> None:
        self.maxcompute_conn_id = maxcompute_conn_id
        super().__init__(alibabacloud_conn_id=maxcompute_conn_id, **kwargs)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        connection_form_widgets = super().get_connection_form_widgets()

        connection_form_widgets["project"] = StringField(
            lazy_gettext("Project"),
            widget=BS3TextFieldWidget(),
        )
        connection_form_widgets["endpoint"] = StringField(
            lazy_gettext("Endpoint"),
            widget=BS3TextFieldWidget(),
        )

        return connection_form_widgets

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }

    @property
    def project(self) -> str:
        """
        Returns project ID.

        :return: ID of the project
        """
        return self._get_field("project")

    @property
    def endpoint(self) -> str:
        """
        Returns MaxCompute Endpoint.

        :return: Endpoint of the MaxCompute project
        """
        return self._get_field("endpoint")

    @fallback_to_default_project_endpoint
    def get_client(self, *, project: str, endpoint: str) -> ODPS:
        """
        Get an authenticated MaxCompute ODPS Client.

        :param project_id: Project ID for the project which the client acts on behalf of.
        :param location: Default location for jobs / datasets / tables.
        """
        creds = self.get_access_key_credential()

        return ODPS(
            creds.access_key_id,
            creds.access_key_secret,
            project=project,
            endpoint=endpoint,
        )

    @fallback_to_default_project_endpoint
    def run_sql(
        self,
        *,
        sql: str,
        project: str | None = None,
        endpoint: str | None = None,
        priority: int | None = None,
        running_cluster: str | None = None,
        hints: dict[str, Any] | None = None,
        aliases: dict[str, str] | None = None,
        default_schema: str | None = None,
        quota_name: str | None = None,
    ) -> Instance:
        """
        Run a given SQL statement in MaxCompute.

        The method will submit your SQL statement to MaxCompute
        and return the corresponding task Instance object.

        .. seealso:: https://pyodps.readthedocs.io/en/latest/base-sql.html#execute-sql

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
        :return: The MaxCompute task instance.
        """
        client = self.get_client(project=project, endpoint=endpoint)

        if priority is None and hints is not None:
            priority = hints.get("odps.instance.priority")

        return client.run_sql(
            sql=sql,
            priority=priority,
            running_cluster=running_cluster,
            hints=hints,
            aliases=aliases,
            default_schema=default_schema,
            quota_name=quota_name,
        )

    @fallback_to_default_project_endpoint
    def get_instance(
        self,
        *,
        instance_id: str,
        project: str | None = None,
        endpoint: str | None = None,
    ) -> Instance:
        """
        Get a MaxCompute task instance.

        .. seealso:: https://pyodps.readthedocs.io/en/latest/base-instances.html#instances

        :param instance_id: The ID of the instance to get.
        :param project: The project ID to use.
        :param endpoint: The endpoint to use.
        :return: The MaxCompute task instance.
        :raises ValueError: If the instance does not exist.
        """
        client = self.get_client(project=project, endpoint=endpoint)

        return client.get_instance(id_=instance_id, project=project)

    @fallback_to_default_project_endpoint
    def stop_instance(
        self,
        *,
        instance_id: str,
        project: str | None = None,
        endpoint: str | None = None,
    ) -> None:
        """
        Stop a MaxCompute task instance.

        :param instance_id: The ID of the instance to stop.
        :param project: The project ID to use.
        :param endpoint: The endpoint to use.
        """
        client = self.get_client(project=project, endpoint=endpoint)

        try:
            client.stop_instance(id_=instance_id, project=project)
            self.log.info("Instance %s stop requested.", instance_id)
        except Exception:
            self.log.exception("Failed to stop instance %s.", instance_id)
            raise
