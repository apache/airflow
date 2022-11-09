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
"""This module contains a Google Cloud SQL Hook."""
from __future__ import annotations

import errno
import json
import os
import os.path
import platform
import random
import re
import shutil
import socket
import string
import subprocess
import time
import uuid
from inspect import signature
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import gettempdir
from typing import Any, Sequence
from urllib.parse import quote_plus

import httpx
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException

# Number of retries - used by googleapiclient method calls to perform retries
# For requests that are "retriable"
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook, get_field
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

UNIX_PATH_MAX = 108

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 20


class CloudSqlOperationStatus:
    """Helper class with operation statuses."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"


class CloudSQLHook(GoogleBaseHook):
    """
    Hook for Google Cloud SQL APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param api_version: This is the version of the api.
    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param delegate_to: This performs a task on one host with reference to other hosts.
    :param impersonation_chain: This is the optional service account to impersonate using short term
        credentials.
    """

    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_sql_default"
    conn_type = "gcpcloudsql"
    hook_name = "Google Cloud SQL"

    def __init__(
        self,
        api_version: str,
        gcp_conn_id: str = default_conn_name,
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version
        self._conn = None

    def get_conn(self) -> Resource:
        """
        Retrieves connection to Cloud SQL.

        :return: Google Cloud SQL services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("sqladmin", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(self, instance: str, project_id: str) -> dict:
        """
        Retrieves a resource containing information about a Cloud SQL instance.

        :param instance: Database instance ID. This does not include the project ID.
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: A Cloud SQL instance resource.
        """
        return (
            self.get_conn()
            .instances()
            .get(project=project_id, instance=instance)
            .execute(num_retries=self.num_retries)
        )

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.operation_in_progress_retry()
    def create_instance(self, body: dict, project_id: str) -> None:
        """
        Creates a new Cloud SQL instance.

        :param body: Body required by the Cloud SQL insert API, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert#request-body.
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .instances()
            .insert(project=project_id, body=body)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.operation_in_progress_retry()
    def patch_instance(self, body: dict, instance: str, project_id: str) -> None:
        """
        Updates settings of a Cloud SQL instance.

        Caution: This is not a partial update, so you must include values for
        all the settings that you want to retain.

        :param body: Body required by the Cloud SQL patch API, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch#request-body.
        :param instance: Cloud SQL instance ID. This does not include the project ID.
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .instances()
            .patch(project=project_id, instance=instance, body=body)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.operation_in_progress_retry()
    def delete_instance(self, instance: str, project_id: str) -> None:
        """
        Deletes a Cloud SQL instance.

        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :param instance: Cloud SQL instance ID. This does not include the project ID.
        :return: None
        """
        response = (
            self.get_conn()
            .instances()
            .delete(project=project_id, instance=instance)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_database(self, instance: str, database: str, project_id: str) -> dict:
        """
        Retrieves a database resource from a Cloud SQL instance.

        :param instance: Database instance ID. This does not include the project ID.
        :param database: Name of the database in the instance.
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: A Cloud SQL database resource, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases#resource.
        """
        return (
            self.get_conn()
            .databases()
            .get(project=project_id, instance=instance, database=database)
            .execute(num_retries=self.num_retries)
        )

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.operation_in_progress_retry()
    def create_database(self, instance: str, body: dict, project_id: str) -> None:
        """
        Creates a new database inside a Cloud SQL instance.

        :param instance: Database instance ID. This does not include the project ID.
        :param body: The request body, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert#request-body.
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .databases()
            .insert(project=project_id, instance=instance, body=body)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.operation_in_progress_retry()
    def patch_database(
        self,
        instance: str,
        database: str,
        body: dict,
        project_id: str,
    ) -> None:
        """
        Updates a database resource inside a Cloud SQL instance.

        This method supports patch semantics.
        See https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch.

        :param instance: Database instance ID. This does not include the project ID.
        :param database: Name of the database to be updated in the instance.
        :param body: The request body, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert#request-body.
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .databases()
            .patch(project=project_id, instance=instance, database=database, body=body)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.operation_in_progress_retry()
    def delete_database(self, instance: str, database: str, project_id: str) -> None:
        """
        Deletes a database from a Cloud SQL instance.

        :param instance: Database instance ID. This does not include the project ID.
        :param database: Name of the database to be deleted in the instance.
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .databases()
            .delete(project=project_id, instance=instance, database=database)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.operation_in_progress_retry()
    def export_instance(self, instance: str, body: dict, project_id: str) -> None:
        """
        Exports data from a Cloud SQL instance to a Cloud Storage bucket as a SQL dump
        or CSV file.

        :param instance: Database instance ID of the Cloud SQL instance. This does not include the
            project ID.
        :param body: The request body, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: None
        """
        response = (
            self.get_conn()
            .instances()
            .export(project=project_id, instance=instance, body=body)
            .execute(num_retries=self.num_retries)
        )
        operation_name = response["name"]
        self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)

    @GoogleBaseHook.fallback_to_default_project_id
    def import_instance(self, instance: str, body: dict, project_id: str) -> None:
        """
        Imports data into a Cloud SQL instance from a SQL dump or CSV file in
        Cloud Storage.

        :param instance: Database instance ID. This does not include the
            project ID.
        :param body: The request body, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/import#request-body
        :param project_id: Project ID of the project that contains the instance. If set
            to None or missing, the default project_id from the Google Cloud connection is used.
        :return: None
        """
        try:
            response = (
                self.get_conn()
                .instances()
                .import_(project=project_id, instance=instance, body=body)
                .execute(num_retries=self.num_retries)
            )
            operation_name = response["name"]
            self._wait_for_operation_to_complete(project_id=project_id, operation_name=operation_name)
        except HttpError as ex:
            raise AirflowException(f"Importing instance {instance} failed: {ex.content}")

    def _wait_for_operation_to_complete(self, project_id: str, operation_name: str) -> None:
        """
        Waits for the named operation to complete - checks status of the
        asynchronous call.

        :param project_id: Project ID of the project that contains the instance.
        :param operation_name: Name of the operation.
        :return: None
        """
        service = self.get_conn()
        while True:
            operation_response = (
                service.operations()
                .get(project=project_id, operation=operation_name)
                .execute(num_retries=self.num_retries)
            )
            if operation_response.get("status") == CloudSqlOperationStatus.DONE:
                error = operation_response.get("error")
                if error:
                    # Extracting the errors list as string and trimming square braces
                    error_msg = str(error.get("errors"))[1:-1]
                    raise AirflowException(error_msg)
                # No meaningful info to return from the response in case of success
                return
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)


CLOUD_SQL_PROXY_DOWNLOAD_URL = "https://dl.google.com/cloudsql/cloud_sql_proxy.{}.{}"
CLOUD_SQL_PROXY_VERSION_DOWNLOAD_URL = (
    "https://storage.googleapis.com/cloudsql-proxy/{}/cloud_sql_proxy.{}.{}"
)


class CloudSqlProxyRunner(LoggingMixin):
    """
    Downloads and runs cloud-sql-proxy as subprocess of the Python process.

    The cloud-sql-proxy needs to be downloaded and started before we can connect
    to the Google Cloud SQL instance via database connection. It establishes
    secure tunnel connection to the database. It authorizes using the
    Google Cloud credentials that are passed by the configuration.

    More details about the proxy can be found here:
    https://cloud.google.com/sql/docs/mysql/sql-proxy

    :param path_prefix: Unique path prefix where proxy will be downloaded and
        directories created for unix sockets.
    :param instance_specification: Specification of the instance to connect the
        proxy to. It should be specified in the form that is described in
        https://cloud.google.com/sql/docs/mysql/sql-proxy#multiple-instances in
        -instances parameter (typically in the form of ``<project>:<region>:<instance>``
        for UNIX socket connections and in the form of
        ``<project>:<region>:<instance>=tcp:<port>`` for TCP connections.
    :param gcp_conn_id: Id of Google Cloud connection to use for
        authentication
    :param project_id: Optional id of the Google Cloud project to connect to - it overwrites
        default project id taken from the Google Cloud connection.
    :param sql_proxy_version: Specific version of SQL proxy to download
        (for example 'v1.13'). By default latest version is downloaded.
    :param sql_proxy_binary_path: If specified, then proxy will be
        used from the path specified rather than dynamically generated. This means
        that if the binary is not present in that path it will also be downloaded.
    """

    def __init__(
        self,
        path_prefix: str,
        instance_specification: str,
        gcp_conn_id: str = "google_cloud_default",
        project_id: str | None = None,
        sql_proxy_version: str | None = None,
        sql_proxy_binary_path: str | None = None,
    ) -> None:
        super().__init__()
        self.path_prefix = path_prefix
        if not self.path_prefix:
            raise AirflowException("The path_prefix must not be empty!")
        self.sql_proxy_was_downloaded = False
        self.sql_proxy_version = sql_proxy_version
        self.download_sql_proxy_dir = None
        self.sql_proxy_process: Popen | None = None
        self.instance_specification = instance_specification
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.command_line_parameters: list[str] = []
        self.cloud_sql_proxy_socket_directory = self.path_prefix
        self.sql_proxy_path = (
            sql_proxy_binary_path if sql_proxy_binary_path else self.path_prefix + "_cloud_sql_proxy"
        )
        self.credentials_path = self.path_prefix + "_credentials.json"
        self._build_command_line_parameters()

    def _build_command_line_parameters(self) -> None:
        self.command_line_parameters.extend(["-dir", self.cloud_sql_proxy_socket_directory])
        self.command_line_parameters.extend(["-instances", self.instance_specification])

    @staticmethod
    def _is_os_64bit() -> bool:
        return platform.machine().endswith("64")

    def _download_sql_proxy_if_needed(self) -> None:
        if os.path.isfile(self.sql_proxy_path):
            self.log.info("cloud-sql-proxy is already present")
            return
        system = platform.system().lower()
        processor = os.uname().machine
        if processor == "x86_64":
            processor = "amd64"
        if not self.sql_proxy_version:
            download_url = CLOUD_SQL_PROXY_DOWNLOAD_URL.format(system, processor)
        else:
            download_url = CLOUD_SQL_PROXY_VERSION_DOWNLOAD_URL.format(
                self.sql_proxy_version, system, processor
            )
        proxy_path_tmp = self.sql_proxy_path + ".tmp"
        self.log.info("Downloading cloud_sql_proxy from %s to %s", download_url, proxy_path_tmp)
        # httpx has a breaking API change (follow_redirects vs allow_redirects)
        # and this should work with both versions (cf. issue #20088)
        if "follow_redirects" in signature(httpx.get).parameters.keys():
            response = httpx.get(download_url, follow_redirects=True)
        else:
            response = httpx.get(download_url, allow_redirects=True)  # type: ignore[call-arg]
        # Downloading to .tmp file first to avoid case where partially downloaded
        # binary is used by parallel operator which uses the same fixed binary path
        with open(proxy_path_tmp, "wb") as file:
            file.write(response.content)
        if response.status_code != 200:
            raise AirflowException(
                "The cloud-sql-proxy could not be downloaded. "
                f"Status code = {response.status_code}. Reason = {response.reason_phrase}"
            )

        self.log.info("Moving sql_proxy binary from %s to %s", proxy_path_tmp, self.sql_proxy_path)
        shutil.move(proxy_path_tmp, self.sql_proxy_path)
        os.chmod(self.sql_proxy_path, 0o744)  # Set executable bit
        self.sql_proxy_was_downloaded = True

    def _get_credential_parameters(self) -> list[str]:
        extras = GoogleBaseHook.get_connection(conn_id=self.gcp_conn_id).extra_dejson
        key_path = get_field(extras, "key_path")
        keyfile_dict = get_field(extras, "keyfile_dict")
        if key_path:
            credential_params = ["-credential_file", key_path]
        elif keyfile_dict:
            keyfile_content = keyfile_dict if isinstance(keyfile_dict, dict) else json.loads(keyfile_dict)
            self.log.info("Saving credentials to %s", self.credentials_path)
            with open(self.credentials_path, "w") as file:
                json.dump(keyfile_content, file)
            credential_params = ["-credential_file", self.credentials_path]
        else:
            self.log.info(
                "The credentials are not supplied by neither key_path nor "
                "keyfile_dict of the gcp connection %s. Falling back to "
                "default activated account",
                self.gcp_conn_id,
            )
            credential_params = []

        if not self.instance_specification:
            project_id = get_field(extras, "project")
            if self.project_id:
                project_id = self.project_id
            if not project_id:
                raise AirflowException(
                    "For forwarding all instances, the project id "
                    "for Google Cloud should be provided either "
                    "by project_id extra in the Google Cloud connection or by "
                    "project_id provided in the operator."
                )
            credential_params.extend(["-projects", project_id])
        return credential_params

    def start_proxy(self) -> None:
        """
        Starts Cloud SQL Proxy.

        You have to remember to stop the proxy if you started it!
        """
        self._download_sql_proxy_if_needed()
        if self.sql_proxy_process:
            raise AirflowException(f"The sql proxy is already running: {self.sql_proxy_process}")
        else:
            command_to_run = [self.sql_proxy_path]
            command_to_run.extend(self.command_line_parameters)
            self.log.info("Creating directory %s", self.cloud_sql_proxy_socket_directory)
            Path(self.cloud_sql_proxy_socket_directory).mkdir(parents=True, exist_ok=True)
            command_to_run.extend(self._get_credential_parameters())
            self.log.info("Running the command: `%s`", " ".join(command_to_run))

            self.sql_proxy_process = Popen(command_to_run, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            self.log.info("The pid of cloud_sql_proxy: %s", self.sql_proxy_process.pid)
            while True:
                line = (
                    self.sql_proxy_process.stderr.readline().decode("utf-8")
                    if self.sql_proxy_process.stderr
                    else ""
                )
                return_code = self.sql_proxy_process.poll()
                if line == "" and return_code is not None:
                    self.sql_proxy_process = None
                    raise AirflowException(
                        f"The cloud_sql_proxy finished early with return code {return_code}!"
                    )
                if line != "":
                    self.log.info(line)
                if "googleapi: Error" in line or "invalid instance name:" in line:
                    self.stop_proxy()
                    raise AirflowException(f"Error when starting the cloud_sql_proxy {line}!")
                if "Ready for new connections" in line:
                    return

    def stop_proxy(self) -> None:
        """
        Stops running proxy.

        You should stop the proxy after you stop using it.
        """
        if not self.sql_proxy_process:
            raise AirflowException("The sql proxy is not started yet")
        else:
            self.log.info("Stopping the cloud_sql_proxy pid: %s", self.sql_proxy_process.pid)
            self.sql_proxy_process.kill()
            self.sql_proxy_process = None
        # Cleanup!
        self.log.info("Removing the socket directory: %s", self.cloud_sql_proxy_socket_directory)
        shutil.rmtree(self.cloud_sql_proxy_socket_directory, ignore_errors=True)
        if self.sql_proxy_was_downloaded:
            self.log.info("Removing downloaded proxy: %s", self.sql_proxy_path)
            # Silently ignore if the file has already been removed (concurrency)
            try:
                os.remove(self.sql_proxy_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
        else:
            self.log.info("Skipped removing proxy - it was not downloaded: %s", self.sql_proxy_path)
        if os.path.isfile(self.credentials_path):
            self.log.info("Removing generated credentials file %s", self.credentials_path)
            # Here file cannot be delete by concurrent task (each task has its own copy)
            os.remove(self.credentials_path)

    def get_proxy_version(self) -> str | None:
        """Returns version of the Cloud SQL Proxy."""
        self._download_sql_proxy_if_needed()
        command_to_run = [self.sql_proxy_path]
        command_to_run.extend(["--version"])
        command_to_run.extend(self._get_credential_parameters())
        result = subprocess.check_output(command_to_run).decode("utf-8")
        pattern = re.compile("^.*[V|v]ersion ([^;]*);.*$")
        matched = pattern.match(result)
        if matched:
            return matched.group(1)
        else:
            return None

    def get_socket_path(self) -> str:
        """
        Retrieves UNIX socket path used by Cloud SQL Proxy.

        :return: The dynamically generated path for the socket created by the proxy.
        """
        return self.cloud_sql_proxy_socket_directory + "/" + self.instance_specification


CONNECTION_URIS: dict[str, dict[str, dict[str, str]]] = {
    "postgres": {
        "proxy": {
            "tcp": "postgresql://{user}:{password}@127.0.0.1:{proxy_port}/{database}",
            "socket": "postgresql://{user}:{password}@{socket_path}/{database}",
        },
        "public": {
            "ssl": "postgresql://{user}:{password}@{public_ip}:{public_port}/{database}?"
            "sslmode=verify-ca&"
            "sslcert={client_cert_file}&"
            "sslkey={client_key_file}&"
            "sslrootcert={server_ca_file}",
            "non-ssl": "postgresql://{user}:{password}@{public_ip}:{public_port}/{database}",
        },
    },
    "mysql": {
        "proxy": {
            "tcp": "mysql://{user}:{password}@127.0.0.1:{proxy_port}/{database}",
            "socket": "mysql://{user}:{password}@localhost/{database}?unix_socket={socket_path}",
        },
        "public": {
            "ssl": "mysql://{user}:{password}@{public_ip}:{public_port}/{database}?ssl={ssl_spec}",
            "non-ssl": "mysql://{user}:{password}@{public_ip}:{public_port}/{database}",
        },
    },
}

CLOUD_SQL_VALID_DATABASE_TYPES = ["postgres", "mysql"]


class CloudSQLDatabaseHook(BaseHook):
    """Serves DB connection configuration for Google Cloud SQL (Connections
    of *gcpcloudsqldb://* type).

    The hook is a "meta" one. It does not perform an actual connection.
    It is there to retrieve all the parameters configured in gcpcloudsql:// connection,
    start/stop Cloud SQL Proxy if needed, dynamically generate Postgres or MySQL
    connection in the database and return an actual Postgres or MySQL hook.
    The returned Postgres/MySQL hooks are using direct connection or Cloud SQL
    Proxy socket/TCP as configured.

    Main parameters of the hook are retrieved from the standard URI components:

    * **user** - User name to authenticate to the database (from login of the URI).
    * **password** - Password to authenticate to the database (from password of the URI).
    * **public_ip** - IP to connect to for public connection (from host of the URI).
    * **public_port** - Port to connect to for public connection (from port of the URI).
    * **database** - Database to connect to (from schema of the URI).

    Remaining parameters are retrieved from the extras (URI query parameters):

    * **project_id** - Optional, Google Cloud project where the Cloud SQL
       instance exists. If missing, default project id passed is used.
    * **instance** -  Name of the instance of the Cloud SQL database instance.
    * **location** - The location of the Cloud SQL instance (for example europe-west1).
    * **database_type** - The type of the database instance (MySQL or Postgres).
    * **use_proxy** - (default False) Whether SQL proxy should be used to connect to Cloud
      SQL DB.
    * **use_ssl** - (default False) Whether SSL should be used to connect to Cloud SQL DB.
      You cannot use proxy and SSL together.
    * **sql_proxy_use_tcp** - (default False) If set to true, TCP is used to connect via
      proxy, otherwise UNIX sockets are used.
    * **sql_proxy_binary_path** - Optional path to Cloud SQL Proxy binary. If the binary
      is not specified or the binary is not present, it is automatically downloaded.
    * **sql_proxy_version** -  Specific version of the proxy to download (for example
      v1.13). If not specified, the latest version is downloaded.
    * **sslcert** - Path to client certificate to authenticate when SSL is used.
    * **sslkey** - Path to client private key to authenticate when SSL is used.
    * **sslrootcert** - Path to server's certificate to authenticate when SSL is used.

    :param gcp_cloudsql_conn_id: URL of the connection
    :param gcp_conn_id: The connection ID used to connect to Google Cloud for
        cloud-sql-proxy authentication.
    :param default_gcp_project_id: Default project id used if project_id not specified
           in the connection URL
    """

    conn_name_attr = "gcp_cloudsql_conn_id"
    default_conn_name = "google_cloud_sqldb_default"
    conn_type = "gcpcloudsqldb"
    hook_name = "Google Cloud SQL Database"

    _conn = None

    def __init__(
        self,
        gcp_cloudsql_conn_id: str = "google_cloud_sql_default",
        gcp_conn_id: str = "google_cloud_default",
        default_gcp_project_id: str | None = None,
    ) -> None:
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.gcp_cloudsql_conn_id = gcp_cloudsql_conn_id
        self.cloudsql_connection = self.get_connection(self.gcp_cloudsql_conn_id)
        self.extras = self.cloudsql_connection.extra_dejson
        self.project_id = self.extras.get("project_id", default_gcp_project_id)
        self.instance = self.extras.get("instance")
        self.database = self.cloudsql_connection.schema
        self.location = self.extras.get("location")
        self.database_type = self.extras.get("database_type")
        self.use_proxy = self._get_bool(self.extras.get("use_proxy", "False"))
        self.use_ssl = self._get_bool(self.extras.get("use_ssl", "False"))
        self.sql_proxy_use_tcp = self._get_bool(self.extras.get("sql_proxy_use_tcp", "False"))
        self.sql_proxy_version = self.extras.get("sql_proxy_version")
        self.sql_proxy_binary_path = self.extras.get("sql_proxy_binary_path")
        self.user = self.cloudsql_connection.login
        self.password = self.cloudsql_connection.password
        self.public_ip = self.cloudsql_connection.host
        self.public_port = self.cloudsql_connection.port
        self.sslcert = self.extras.get("sslcert")
        self.sslkey = self.extras.get("sslkey")
        self.sslrootcert = self.extras.get("sslrootcert")
        # Port and socket path and db_hook are automatically generated
        self.sql_proxy_tcp_port = None
        self.sql_proxy_unique_path: str | None = None
        self.db_hook: PostgresHook | MySqlHook | None = None
        self.reserved_tcp_socket: socket.socket | None = None
        # Generated based on clock + clock sequence. Unique per host (!).
        # This is important as different hosts share the database
        self.db_conn_id = str(uuid.uuid1())
        self._validate_inputs()

    @staticmethod
    def _get_bool(val: Any) -> bool:
        if val == "False" or val is False:
            return False
        return True

    @staticmethod
    def _check_ssl_file(file_to_check, name) -> None:
        if not file_to_check:
            raise AirflowException(f"SSL connections requires {name} to be set")
        if not os.path.isfile(file_to_check):
            raise AirflowException(f"The {file_to_check} must be a readable file")

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required extra 'project_id' is empty")
        if not self.location:
            raise AirflowException("The required extra 'location' is empty or None")
        if not self.instance:
            raise AirflowException("The required extra 'instance' is empty or None")
        if self.database_type not in CLOUD_SQL_VALID_DATABASE_TYPES:
            raise AirflowException(
                f"Invalid database type '{self.database_type}'. "
                f"Must be one of {CLOUD_SQL_VALID_DATABASE_TYPES}"
            )
        if self.use_proxy and self.use_ssl:
            raise AirflowException(
                "Cloud SQL Proxy does not support SSL connections."
                " SSL is not needed as Cloud SQL Proxy "
                "provides encryption on its own"
            )

    def validate_ssl_certs(self) -> None:
        """
        SSL certificates validator.

        :return: None
        """
        if self.use_ssl:
            self._check_ssl_file(self.sslcert, "sslcert")
            self._check_ssl_file(self.sslkey, "sslkey")
            self._check_ssl_file(self.sslrootcert, "sslrootcert")

    def validate_socket_path_length(self) -> None:
        """
        Validates sockets path length.

        :return: None or rises AirflowException
        """
        if self.use_proxy and not self.sql_proxy_use_tcp:
            if self.database_type == "postgres":
                suffix = "/.s.PGSQL.5432"
            else:
                suffix = ""
            expected_path = (
                f"{self._generate_unique_path()}/{self.project_id}:{self.instance}:{self.database}{suffix}"
            )
            if len(expected_path) > UNIX_PATH_MAX:
                self.log.info("Too long (%s) path: %s", len(expected_path), expected_path)
                raise AirflowException(
                    f"The UNIX socket path length cannot exceed {UNIX_PATH_MAX} characters on Linux system. "
                    "Either use shorter instance/database name or switch to TCP connection. "
                    f"The socket path for Cloud SQL proxy is now:{expected_path}"
                )

    @staticmethod
    def _generate_unique_path() -> str:
        """
        We are not using mkdtemp here as the path generated with mkdtemp
        can be close to 60 characters and there is a limitation in
        length of socket path to around 100 characters in total.
        We append project/location/instance to it later and postgres
        appends its own prefix, so we chose a shorter "${tempdir()}[8 random characters]"
        """
        random.seed()
        while True:
            candidate = os.path.join(
                gettempdir(), "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8))
            )
            if not os.path.exists(candidate):
                return candidate

    @staticmethod
    def _quote(value) -> str | None:
        return quote_plus(value) if value else None

    def _generate_connection_uri(self) -> str:
        if self.use_proxy:
            if self.sql_proxy_use_tcp:
                if not self.sql_proxy_tcp_port:
                    self.reserve_free_tcp_port()
            if not self.sql_proxy_unique_path:
                self.sql_proxy_unique_path = self._generate_unique_path()
        if not self.database_type:
            raise ValueError("The database_type should be set")

        database_uris = CONNECTION_URIS[self.database_type]
        ssl_spec = None
        socket_path = None
        if self.use_proxy:
            proxy_uris = database_uris["proxy"]
            if self.sql_proxy_use_tcp:
                format_string = proxy_uris["tcp"]
            else:
                format_string = proxy_uris["socket"]
                socket_path = f"{self.sql_proxy_unique_path}/{self._get_instance_socket_name()}"
        else:
            public_uris = database_uris["public"]
            if self.use_ssl:
                format_string = public_uris["ssl"]
                ssl_spec = {"cert": self.sslcert, "key": self.sslkey, "ca": self.sslrootcert}
            else:
                format_string = public_uris["non-ssl"]
        if not self.user:
            raise AirflowException("The login parameter needs to be set in connection")
        if not self.public_ip:
            raise AirflowException("The location parameter needs to be set in connection")
        if not self.password:
            raise AirflowException("The password parameter needs to be set in connection")
        if not self.database:
            raise AirflowException("The database parameter needs to be set in connection")

        connection_uri = format_string.format(
            user=quote_plus(self.user) if self.user else "",
            password=quote_plus(self.password) if self.password else "",
            database=quote_plus(self.database) if self.database else "",
            public_ip=self.public_ip,
            public_port=self.public_port,
            proxy_port=self.sql_proxy_tcp_port,
            socket_path=self._quote(socket_path),
            ssl_spec=self._quote(json.dumps(ssl_spec)) if ssl_spec else "",
            client_cert_file=self._quote(self.sslcert) if self.sslcert else "",
            client_key_file=self._quote(self.sslkey) if self.sslcert else "",
            server_ca_file=self._quote(self.sslrootcert if self.sslcert else ""),
        )
        self.log.info(
            "DB connection URI %s",
            connection_uri.replace(
                quote_plus(self.password) if self.password else "PASSWORD", "XXXXXXXXXXXX"
            ),
        )
        return connection_uri

    def _get_instance_socket_name(self) -> str:
        return self.project_id + ":" + self.location + ":" + self.instance

    def _get_sqlproxy_instance_specification(self) -> str:
        instance_specification = self._get_instance_socket_name()
        if self.sql_proxy_use_tcp:
            instance_specification += "=tcp:" + str(self.sql_proxy_tcp_port)
        return instance_specification

    def create_connection(self) -> Connection:
        """
        Create Connection object, according to whether it uses proxy, TCP, UNIX sockets, SSL.
        Connection ID will be randomly generated.
        """
        uri = self._generate_connection_uri()
        connection = Connection(conn_id=self.db_conn_id, uri=uri)
        self.log.info("Creating connection %s", self.db_conn_id)
        return connection

    def get_sqlproxy_runner(self) -> CloudSqlProxyRunner:
        """
        Retrieve Cloud SQL Proxy runner. It is used to manage the proxy
        lifecycle per task.

        :return: The Cloud SQL Proxy runner.
        """
        if not self.use_proxy:
            raise ValueError("Proxy runner can only be retrieved in case of use_proxy = True")
        if not self.sql_proxy_unique_path:
            raise ValueError("The sql_proxy_unique_path should be set")
        return CloudSqlProxyRunner(
            path_prefix=self.sql_proxy_unique_path,
            instance_specification=self._get_sqlproxy_instance_specification(),
            project_id=self.project_id,
            sql_proxy_version=self.sql_proxy_version,
            sql_proxy_binary_path=self.sql_proxy_binary_path,
            gcp_conn_id=self.gcp_conn_id,
        )

    def get_database_hook(self, connection: Connection) -> PostgresHook | MySqlHook:
        """
        Retrieve database hook. This is the actual Postgres or MySQL database hook
        that uses proxy or connects directly to the Google Cloud SQL database.
        """
        if self.database_type == "postgres":
            db_hook: PostgresHook | MySqlHook = PostgresHook(connection=connection, schema=self.database)
        else:
            db_hook = MySqlHook(connection=connection, schema=self.database)
        self.db_hook = db_hook
        return db_hook

    def cleanup_database_hook(self) -> None:
        """Clean up database hook after it was used."""
        if self.database_type == "postgres":
            if not self.db_hook:
                raise ValueError("The db_hook should be set")
            if not isinstance(self.db_hook, PostgresHook):
                raise ValueError(f"The db_hook should be PostgresHook and is {type(self.db_hook)}")
            conn = getattr(self.db_hook, "conn")
            if conn and conn.notices:
                for output in self.db_hook.conn.notices:
                    self.log.info(output)

    def reserve_free_tcp_port(self) -> None:
        """Reserve free TCP port to be used by Cloud SQL Proxy"""
        self.reserved_tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.reserved_tcp_socket.bind(("127.0.0.1", 0))
        self.sql_proxy_tcp_port = self.reserved_tcp_socket.getsockname()[1]

    def free_reserved_port(self) -> None:
        """Free TCP port. Makes it immediately ready to be used by Cloud SQL Proxy."""
        if self.reserved_tcp_socket:
            self.reserved_tcp_socket.close()
            self.reserved_tcp_socket = None
