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

import time
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.utils.helpers import exactly_one

# tableauserverclient is imported lazily inside the methods that use it rather than at module level:
# importing it is expensive (it self-instruments via beartype's import hook on first import), and that
# cost would otherwise be paid whenever the ProvidersManager imports this hook module just to discover
# provider metadata -- which never instantiates the hook.
if TYPE_CHECKING:
    from tableauserverclient import Pager
    from tableauserverclient.server import Auth


def parse_boolean(val: str) -> str | bool:
    """
    Try to parse a string into boolean.

    The string is returned as-is if it does not look like a boolean value.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
    return val


class TableauJobFailedException(AirflowException):
    """An exception that indicates that a Job failed to complete."""


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
    Connects to the Tableau Server Instance and allows to communicate with it.

    Can be used as a context manager: automatically authenticates the connection
    when opened and signs out when closed.

    .. seealso:: https://tableau.github.io/server-client-python/docs/

    :param site_id: The id of the site where the workbook belongs to.
        It will connect to the default site if you don't provide an id.
    :param tableau_conn_id: The :ref:`Tableau Connection id <howto/connection:tableau>`
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
        server_address = f"{self.conn.schema}://{self.conn.host}" if self.conn.schema else self.conn.host
        from tableauserverclient import Server

        self.server = Server(server_address)
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

        :return: an authorized Tableau Server Context Manager object.
        """
        extra = self.conn.extra_dejson
        password_auth_set = self.conn.login and self.conn.password
        jwt_auth_set = extra.get("auth") == "jwt"

        if password_auth_set and jwt_auth_set:
            raise AirflowException(
                "Username/password authentication and JWT authentication cannot be used simultaneously. Please specify only one authentication method."
            )
        if password_auth_set:
            return self._auth_via_password()
        if jwt_auth_set:
            if not exactly_one(jwt_file := "jwt_file" in extra, jwt_token := "jwt_token" in extra):
                msg = (
                    "When auth set to 'jwt' then expected exactly one parameter 'jwt_file' or 'jwt_token'"
                    " in connection extra, but "
                )
                if jwt_file and jwt_token:
                    msg += "provided both."
                else:
                    msg += "none of them provided."
                raise ValueError(msg)

            if jwt_file:
                self.jwt_token = Path(extra["jwt_file"]).read_text()
            else:
                self.jwt_token = extra["jwt_token"]
            return self._auth_via_jwt()
        raise NotImplementedError("No Authentication method found for given Credentials!")

    def _auth_via_password(self) -> Auth.contextmgr:
        from tableauserverclient import TableauAuth

        tableau_auth = TableauAuth(
            username=cast("str", self.conn.login),
            password=cast("str", self.conn.password),
            site_id=self.site_id,
        )
        return self.server.auth.sign_in(tableau_auth)

    def _auth_via_jwt(self) -> Auth.contextmgr:
        from tableauserverclient import JWTAuth

        jwt_auth = JWTAuth(jwt=self.jwt_token, site_id=self.site_id)
        return self.server.auth.sign_in(jwt_auth)

    def get_all(self, resource_name: str) -> Pager:
        """
        Get all items of the given resource.

        .. see also:: https://tableau.github.io/server-client-python/docs/page-through-results

        :param resource_name: The name of the resource to paginate.
            For example: jobs or workbooks.
        :return: all items by returning a Pager.
        """
        from tableauserverclient import Pager

        try:
            resource = getattr(self.server, resource_name)
        except AttributeError:
            raise ValueError(f"Resource name {resource_name} is not found.")
        return Pager(resource.get)

    def get_job_status(self, job_id: str) -> TableauJobFinishCode:
        """
        Get the current state of a defined Tableau Job.

        .. see also:: https://tableau.github.io/server-client-python/docs/api-ref#jobs

        :param job_id: The id of the job to check.
        :return: An Enum that describe the Tableau job's return code
        """
        return TableauJobFinishCode(int(self.server.jobs.get_by_id(job_id).finish_code))

    def wait_for_state(
        self,
        job_id: str,
        target_state: TableauJobFinishCode,
        check_interval: float,
        timeout: float | None = None,
        exponential_backoff: bool = False,
        max_check_interval: float | None = None,
    ) -> bool:
        """
        Wait until the current state of a defined Tableau Job is target_state or different from PENDING.

        :param job_id: The id of the job to check.
        :param target_state: Enum that describe the Tableau job's target state
        :param check_interval: time in seconds that the job should wait in
            between each instance state checks until operation is completed
        :param timeout: maximum total time in seconds to keep polling the job before giving up.
            ``None`` (the default) waits indefinitely until the job leaves the PENDING state.
            This is a soft bound: an in-flight wait between checks is not interrupted, so a large
            ``check_interval`` may overshoot ``timeout`` by up to one interval.
        :param exponential_backoff: when ``True`` the wait between checks grows by 50% each time,
            starting from ``check_interval``, instead of staying fixed.
        :param max_check_interval: maximum interval in seconds between two consecutive checks
            when ``exponential_backoff`` is enabled. ``None`` leaves the growth uncapped.
        :return: return True if the job is equal to the target_status, False otherwise.
        :raises TimeoutError: if ``timeout`` elapses while the job is still PENDING.
        """
        start = time.monotonic()
        current_interval = check_interval
        while True:
            finish_code = self.get_job_status(job_id=job_id)
            if finish_code != TableauJobFinishCode.PENDING or finish_code == target_state:
                break
            if timeout is not None and time.monotonic() - start >= timeout:
                raise TimeoutError(f"Tableau job {job_id} is still PENDING after {timeout} seconds.")
            self.log.info("job state: %s; checking again in %s seconds", finish_code, current_interval)
            time.sleep(current_interval)
            if exponential_backoff:
                current_interval *= 1.5
                if max_check_interval is not None:
                    current_interval = min(current_interval, max_check_interval)

        return finish_code == target_state
