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

from airflow.exceptions import AirflowException
from airflow.providers.alibaba.cloud.hooks.base_alibaba import AlibabaBaseHook

if TYPE_CHECKING:
    from odps.models import Instance

RT = TypeVar("RT")


def fallback_to_default_project_endpoint(func: Callable[..., RT]) -> Callable[..., RT]:
    """
    Provide fallback for Maxcompute project and endpoint. To be used as a decorator.

    If the project or endpoint is None it will be replaced with the project from the
    connection extra definition. Project id can be specified
    either via project_id kwarg or via first parameter in positional args.

    :param func: function to wrap
    :return: result of the function call
    """

    @functools.wraps(func)
    def inner_wrapper(self, *args, **kwargs) -> RT:
        if args:
            raise AirflowException("You must use keyword arguments in this methods rather than positional")
        if "project" in kwargs:
            kwargs["project"] = kwargs["project"] or self.project
        else:
            kwargs["project"] = self.project

        if "endpoint" in kwargs:
            kwargs["endpoint"] = kwargs["endpoint"] or self.endpoint
        else:
            kwargs["endpoint"] = self.endpoint

        if not kwargs["project"]:
            raise AirflowException(
                "The project must be passed either as "
                "keyword parameter or as extra "
                "in MaxCompute connection definition. Both are not set!"
            )

        if not kwargs["endpoint"]:
            raise AirflowException(
                "The endpoint must be passed either as "
                "keyword parameter or as extra "
                "in MaxCompute connection definition. Both are not set!"
            )
        return func(self, *args, **kwargs)

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

    def __init__(
        self,
        maxcompute_conn_id: str = "maxcompute_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.maxcompute_conn_id = maxcompute_conn_id
        self.extras: dict = self.get_connection(self.maxcompute_conn_id).extra_dejson

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
    def get_client(self, project: str, endpoint: str) -> ODPS:
        """
        Get an authenticated BigQuery Client.

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
        sql,
        project=None,
        endpoint=None,
        priority=None,
        running_cluster=None,
        hints=None,
        aliases=None,
        default_schema=None,
        quota_name=None,
    ) -> Instance:
        client = self.get_client(project=project, endpoint=endpoint)

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
    def get_instance(self, instance_id, project=None, endpoint=None) -> Instance:
        client = self.get_client(project=project, endpoint=endpoint)

        if client.exist_instance(id_=instance_id, project=project):
            return client.get_instance(id_=instance_id, project=project)
        raise ValueError(f"Instance with id {instance_id} does not exist in project {project}.")
