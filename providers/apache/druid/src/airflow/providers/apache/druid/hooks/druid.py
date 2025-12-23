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
from __future__ import annotations

import time
from collections.abc import Iterable
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any

import requests
from pydruid.db import connect

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models import Connection


class IngestionType(Enum):
    """
    Druid Ingestion Type. Could be Native batch ingestion or SQL-based ingestion.

    https://druid.apache.org/docs/latest/ingestion/index.html
    """

    BATCH = 1
    MSQ = 2


class DruidHook(BaseHook):
    """
    Connection to Druid overlord for ingestion.

    To connect to a Druid cluster that is secured with the druid-basic-security
    extension, add the username and password to the druid ingestion connection.

    :param druid_ingest_conn_id: The connection id to the Druid overlord machine
                                 which accepts index jobs
    :param timeout: The interval between polling
                    the Druid job for the status of the ingestion job.
                    Must be greater than or equal to 1
    :param max_ingestion_time: The maximum ingestion time before assuming the job failed
    :param verify_ssl: Whether to use SSL encryption to submit indexing job. If set to False then checks
                       connection information for path to a CA bundle to use. Defaults to True
    """

    def __init__(
        self,
        druid_ingest_conn_id: str = "druid_ingest_default",
        timeout: int = 1,
        max_ingestion_time: int | None = None,
        verify_ssl: bool = True,
    ) -> None:
        super().__init__()
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.timeout = timeout
        self.max_ingestion_time = max_ingestion_time
        self.header = {"content-type": "application/json"}
        self.verify_ssl = verify_ssl

        if self.timeout < 1:
            raise ValueError("Druid timeout should be equal or greater than 1")

        self.status_endpoint = "druid/indexer/v1/task"

    @cached_property
    def conn(self) -> Connection:
        return self.get_connection(self.druid_ingest_conn_id)  # type: ignore[return-value]

    @property
    def get_connection_type(self) -> str:
        if self.conn.schema:
            conn_type = self.conn.schema
        else:
            conn_type = self.conn.conn_type or "http"
        return conn_type

    def get_conn_url(self, ingestion_type: IngestionType = IngestionType.BATCH) -> str:
        """Get Druid connection url."""
        host = self.conn.host
        port = self.conn.port
        conn_type = self.get_connection_type
        if ingestion_type == IngestionType.BATCH:
            endpoint = self.conn.extra_dejson.get("endpoint", "")
        else:
            endpoint = self.conn.extra_dejson.get("msq_endpoint", "")
        return f"{conn_type}://{host}:{port}/{endpoint}"

    def get_status_url(self, ingestion_type):
        """Return Druid status url."""
        if ingestion_type == IngestionType.MSQ:
            if self.get_connection_type == "druid":
                conn_type = self.conn.extra_dejson.get("schema", "http")
            else:
                conn_type = self.get_connection_type

            status_endpoint = self.conn.extra_dejson.get("status_endpoint", self.status_endpoint)
            return f"{conn_type}://{self.conn.host}:{self.conn.port}/{status_endpoint}"
        return self.get_conn_url(ingestion_type)

    def get_auth(self) -> requests.auth.HTTPBasicAuth | None:
        """
        Return username and password from connections tab as requests.auth.HTTPBasicAuth object.

        If these details have not been set then returns None.
        """
        user = self.conn.login
        password = self.conn.password
        if user is not None and password is not None:
            return requests.auth.HTTPBasicAuth(user, password)
        return None

    def get_verify(self) -> bool | str:
        ca_bundle_path: str | None = self.conn.extra_dejson.get("ca_bundle_path", None)
        if not self.verify_ssl and ca_bundle_path:
            self.log.info("Using CA bundle to verify connection")
            return ca_bundle_path

        return self.verify_ssl

    def submit_indexing_job(
        self, json_index_spec: dict[str, Any] | str, ingestion_type: IngestionType = IngestionType.BATCH
    ) -> None:
        """Submit Druid ingestion job."""
        url = self.get_conn_url(ingestion_type)

        self.log.info("Druid ingestion spec: %s", json_index_spec)
        req_index = requests.post(
            url, data=json_index_spec, headers=self.header, auth=self.get_auth(), verify=self.get_verify()
        )

        code = req_index.status_code
        not_accepted = not (200 <= code < 300)
        if not_accepted:
            self.log.error("Error submitting the Druid job to %s (%s) %s", url, code, req_index.content)
            raise AirflowException(f"Did not get 200 or 202 when submitting the Druid job to {url}")

        req_json = req_index.json()
        # Wait until the job is completed
        if ingestion_type == IngestionType.BATCH:
            druid_task_id = req_json["task"]
        else:
            druid_task_id = req_json["taskId"]
        druid_task_status_url = self.get_status_url(ingestion_type) + f"/{druid_task_id}/status"
        self.log.info("Druid indexing task-id: %s", druid_task_id)

        running = True

        sec = 0
        while running:
            req_status = requests.get(druid_task_status_url, auth=self.get_auth(), verify=self.get_verify())

            self.log.info("Job still running for %s seconds...", sec)

            if self.max_ingestion_time and sec > self.max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                requests.post(
                    f"{url}/{druid_task_id}/shutdown", auth=self.get_auth(), verify=self.get_verify()
                )
                raise AirflowException(f"Druid ingestion took more than {self.max_ingestion_time} seconds")

            time.sleep(self.timeout)

            sec += self.timeout

            status = req_status.json()["status"]["status"]
            if status == "RUNNING":
                running = True
            elif status == "SUCCESS":
                running = False  # Great success!
            elif status == "FAILED":
                raise AirflowException("Druid indexing job failed, check console for more info")
            else:
                raise AirflowException(f"Could not get status of the job, got {status}")

        self.log.info("Successful index")


class DruidDbApiHook(DbApiHook):
    """
    Interact with Druid broker.

    This hook is purely for users to query druid broker.
    For ingestion, please use druidHook.

    :param context: Optional query context parameters to pass to the SQL endpoint.
        Example: ``{"sqlFinalizeOuterSketches": True}``
        See: https://druid.apache.org/docs/latest/querying/sql-query-context/
    """

    conn_name_attr = "druid_broker_conn_id"
    default_conn_name = "druid_broker_default"
    conn_type = "druid"
    hook_name = "Druid"
    supports_autocommit = False

    def __init__(self, context: dict | None = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.context = context or {}

    def get_conn(self) -> connect:
        """Establish a connection to druid broker."""
        conn = self.get_connection(self.get_conn_id())
        druid_broker_conn = connect(
            host=conn.host,
            port=conn.port,
            path=conn.extra_dejson.get("endpoint", "/druid/v2/sql"),
            scheme=conn.extra_dejson.get("schema", "http"),
            user=conn.login,
            password=conn.password,
            context=self.context,
            ssl_verify_cert=conn.extra_dejson.get("ssl_verify_cert", True),
        )
        self.log.info("Get the connection to druid broker on %s using user %s", conn.host, conn.login)
        return druid_broker_conn

    def get_uri(self) -> str:
        """
        Get the connection uri for druid broker.

        e.g: druid://localhost:8082/druid/v2/sql/
        """
        conn = self.get_connection(self.get_conn_id())
        host = conn.host or ""
        if conn.port:
            host += f":{conn.port}"
        conn_type = conn.conn_type or "druid"
        endpoint = conn.extra_dejson.get("endpoint", "druid/v2/sql")
        return f"{conn_type}://{host}/{endpoint}"

    def set_autocommit(self, conn: connect, autocommit: bool) -> None:
        raise NotImplementedError()

    def insert_rows(
        self,
        table: str,
        rows: Iterable[tuple[str]],
        target_fields: Iterable[str] | None = None,
        commit_every: int = 1000,
        replace: bool = False,
        **kwargs: Any,
    ) -> None:
        raise NotImplementedError()
