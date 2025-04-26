# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import time
from enum import Enum
from typing import TYPE_CHECKING, Any

from tableauserverclient import Pager, Server, TableauAuth

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from tableauserverclient.server import Auth


def parse_boolean(val: str) -> str | bool:
    """
    Try to parse a string into a boolean.

    The string is returned as-is if it does not look like a boolean value.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
    return val


class TableauJobFailedException(AirflowException):
    """An exception that indicates that a Tableau job failed to complete."""


class TableauJobFinishCode(Enum):
    """
    The finish code indicates the status of the job.

    .. seealso:: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_job
    """

    PENDING = -1
    SUCCESS = 0
    ERROR = 1
    CANCELED = 2


class TableauHook(BaseHook):
    """
    Connects to the Tableau Server Instance and allows communication with it.

    Can be used as a context manager: automatically authenticates the connection
    when opened and signs out when closed.

    .. seealso:: https://tableau.github.io/server-client-python/docs/

    :param site_id: The ID of the site where the workbook belongs to.
        Connects to the default site if not provided.
    :param tableau_conn_id: The :ref:`Tableau Connection ID <howto/connection:tableau>`
        containing the credentials to authenticate to the Tableau Server.
    """

    conn_name_attr = "tableau_conn_id"
    default_conn_name = "tableau_default"
    conn_type = "tableau"
    hook_name = "Tableau"

    def __init__(self, site_id: str | None = None, tableau_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.tableau_conn_id = tableau_conn_id
        self.conn = self.get_connection(self.tableau_conn_id)
        self.site_id = site_id or self.conn.extra_dejson.get("site_id", "")
        self.server = Server(self.conn.host)
        verify: Any = self.conn.extra_dejson.get("verify", True)
        if isinstance(verify, str):
            verify = parse_boolean(verify)
        self.server.add_http_options(
            options_dict={"verify": verify, "cert": self.conn.extra_dejson.get("cert", None)}
        )
        self.server.use_server_version()
        self.tableau_conn = None

    def __enter__(self):
        if not self.tableau_conn:
            self.tableau_conn = self.get_conn()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.server.auth.sign_out()

    def get_conn(self) -> Auth.contextmgr:
        """
        Sign in to the Tableau Server.

        :return: An authorized Tableau Server Context Manager object.
        """
        if self.conn.login and self.conn.password:
            return self._auth_via_password()
        raise NotImplementedError("No authentication method found for given credentials.")

    def _auth_via_password(self) -> Auth.contextmgr:
        tableau_auth = TableauAuth(
            username=self.conn.login,
            password=self.conn.password,
            site_id=self.site_id,
        )
        return self.server.auth.sign_in(tableau_auth)

    def get_all(self, resource_name: str) -> Pager:
        """
        Get all items of the given resource.

        .. seealso:: https://tableau.github.io/server-client-python/docs/page-through-results

        :param resource_name: The name of the resource to paginate.
            For example: "jobs" or "workbooks".
        :return: All items by returning a Pager.
        """
        try:
            resource = getattr(self.server, resource_name)
        except AttributeError:
            raise ValueError(f"Resource name {resource_name} is not found.")
        return Pager(resource.get)

    def get_job_status(self, job_id: str) -> TableauJobFinishCode:
        """
        Fetch only the finish code for a Tableau Server job.

        :param job_id: The ID of the Tableau job to fetch.
        :return: TableauJobFinishCode enum indicating job status.
        """
        job_item = self.server.jobs.get(job_id)
        return TableauJobFinishCode(job_item.finish_code)

    def get_job(self, job_id: str) -> dict:
        """
        Fetch detailed job information from Tableau Server.

        :param job_id: The ID of the Tableau job to fetch.
        :return: Dictionary containing object type, object name, object ID, and finish code.
        """
        job_item = self.server.jobs.get(job_id)

        resource_type = getattr(job_item, "resource_type", None)
        object_type = resource_type.name if resource_type else "UnknownType"

        object_name = getattr(job_item, "resource_name", "UnknownName")
        object_id = getattr(job_item, "resource_id", "UnknownLUID")
        finish_code = getattr(job_item, "finish_code", None)
        finish_code_value = finish_code.value if finish_code else -1

        return {
            "type": object_type,
            "object_name": object_name,
            "object_id": object_id,
            "finish_code": finish_code_value,
        }

    def wait_for_state(self, job_id: str, target_state: TableauJobFinishCode, check_interval: float) -> bool:
        """
        Wait until the current state of a defined Tableau job is `target_state` or different from PENDING.

        :param job_id: The ID of the job to check.
        :param target_state: Enum that describes the Tableau job's target state.
        :param check_interval: Time in seconds to wait between each state check until operation is completed.
        :return: True if the job reached the target state, False otherwise.
        """
        finish_code = self.get_job_status(job_id=job_id)
        while finish_code == TableauJobFinishCode.PENDING and finish_code != target_state:
            self.log.info("Job state: %s", finish_code)
            time.sleep(check_interval)
            finish_code = self.get_job_status(job_id=job_id)

        return finish_code == target_state
