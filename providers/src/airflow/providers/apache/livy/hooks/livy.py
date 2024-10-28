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
"""This module contains the Apache Livy hook."""

from __future__ import annotations

import asyncio
import json
import re
from enum import Enum
from typing import TYPE_CHECKING, Any, Sequence

import aiohttp
import requests
from aiohttp import ClientResponseError
from asgiref.sync import sync_to_async

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models import Connection


class BatchState(Enum):
    """Batch session states."""

    NOT_STARTED = "not_started"
    STARTING = "starting"
    RUNNING = "running"
    IDLE = "idle"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"


class LivyHook(HttpHook, LoggingMixin):
    """
    Hook for Apache Livy through the REST API.

    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :param extra_options: A dictionary of options passed to Livy.
    :param extra_headers: A dictionary of headers passed to the HTTP request to livy.
    :param auth_type: The auth type for the service.

    .. seealso::
        For more details refer to the Apache Livy API reference:
        https://livy.apache.org/docs/latest/rest-api.html
    """

    TERMINAL_STATES = {
        BatchState.SUCCESS,
        BatchState.DEAD,
        BatchState.KILLED,
        BatchState.ERROR,
    }

    _def_headers = {"Content-Type": "application/json", "Accept": "application/json"}

    conn_name_attr = "livy_conn_id"
    default_conn_name = "livy_default"
    conn_type = "livy"
    hook_name = "Apache Livy"

    def __init__(
        self,
        livy_conn_id: str = default_conn_name,
        extra_options: dict[str, Any] | None = None,
        extra_headers: dict[str, Any] | None = None,
        auth_type: Any | None = None,
    ) -> None:
        super().__init__(http_conn_id=livy_conn_id)
        self.extra_headers = extra_headers or {}
        self.extra_options = extra_options or {}
        if auth_type:
            self.auth_type = auth_type

    def get_conn(self, headers: dict[str, Any] | None = None) -> Any:
        """
        Return http session for use with requests.

        :param headers: additional headers to be passed through as a dictionary
        :return: requests session
        """
        tmp_headers = self._def_headers.copy()  # setting default headers
        if headers:
            tmp_headers.update(headers)
        return super().get_conn(tmp_headers)

    def run_method(
        self,
        endpoint: str,
        method: str = "GET",
        data: Any | None = None,
        headers: dict[str, Any] | None = None,
        retry_args: dict[str, Any] | None = None,
    ) -> Any:
        """
        Wrap HttpHook; allows to change method on the same HttpHook.

        :param method: http method
        :param endpoint: endpoint
        :param data: request payload
        :param headers: headers
        :param retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
        :return: http response
        """
        if method not in ("GET", "POST", "PUT", "DELETE", "HEAD"):
            raise ValueError(f"Invalid http method '{method}'")
        if not self.extra_options:
            self.extra_options = {"check_response": False}

        back_method = self.method
        self.method = method
        try:
            if retry_args:
                result = self.run_with_advanced_retry(
                    endpoint=endpoint,
                    data=data,
                    headers=headers,
                    extra_options=self.extra_options,
                    _retry_args=retry_args,
                )
            else:
                result = self.run(endpoint, data, headers, self.extra_options)

        finally:
            self.method = back_method
        return result

    def post_batch(self, *args: Any, **kwargs: Any) -> int:
        """
        Perform request to submit batch.

        :return: batch session id
        """
        batch_submit_body = json.dumps(self.build_post_batch_body(*args, **kwargs))

        if not self.base_url:
            # need to init self.base_url
            self.get_conn()
        self.log.info("Submitting job %s to %s", batch_submit_body, self.base_url)

        response = self.run_method(
            method="POST",
            endpoint="/batches",
            data=batch_submit_body,
            headers=self.extra_headers,
        )
        self.log.debug("Got response: %s", response.text)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise AirflowException(
                "Could not submit batch. "
                f"Status code: {err.response.status_code}. Message: '{err.response.text}'"
            )

        batch_id = self._parse_post_response(response.json())
        if batch_id is None:
            raise AirflowException("Unable to parse the batch session id")
        self.log.info("Batch submitted with session id: %s", batch_id)

        return batch_id

    def get_batch(self, session_id: int | str) -> dict:
        """
        Fetch info about the specified batch.

        :param session_id: identifier of the batch sessions
        :return: response body
        """
        self._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session %s", session_id)
        response = self.run_method(
            endpoint=f"/batches/{session_id}", headers=self.extra_headers
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning(
                "Got status code %d for session %s", err.response.status_code, session_id
            )
            raise AirflowException(
                f"Unable to fetch batch with id: {session_id}. Message: {err.response.text}"
            )

        return response.json()

    def get_batch_state(
        self, session_id: int | str, retry_args: dict[str, Any] | None = None
    ) -> BatchState:
        """
        Fetch the state of the specified batch.

        :param session_id: identifier of the batch sessions
        :param retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
        :return: batch state
        """
        self._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session %s", session_id)
        response = self.run_method(
            endpoint=f"/batches/{session_id}/state",
            retry_args=retry_args,
            headers=self.extra_headers,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning(
                "Got status code %d for session %s", err.response.status_code, session_id
            )
            raise AirflowException(
                f"Unable to fetch batch with id: {session_id}. Message: {err.response.text}"
            )

        jresp = response.json()
        if "state" not in jresp:
            raise AirflowException(f"Unable to get state for batch with id: {session_id}")
        return BatchState(jresp["state"])

    def delete_batch(self, session_id: int | str) -> dict:
        """
        Delete the specified batch.

        :param session_id: identifier of the batch sessions
        :return: response body
        """
        self._validate_session_id(session_id)

        self.log.info("Deleting batch session %s", session_id)
        response = self.run_method(
            method="DELETE", endpoint=f"/batches/{session_id}", headers=self.extra_headers
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning(
                "Got status code %d for session %s", err.response.status_code, session_id
            )
            raise AirflowException(
                f"Could not kill the batch with session id: {session_id}. Message: {err.response.text}"
            )

        return response.json()

    def get_batch_logs(
        self, session_id: int | str, log_start_position, log_batch_size
    ) -> dict:
        """
        Get the session logs for a specified batch.

        :param session_id: identifier of the batch sessions
        :param log_start_position: Position from where to pull the logs
        :param log_batch_size: Number of lines to pull in one batch

        :return: response body
        """
        self._validate_session_id(session_id)
        log_params = {"from": log_start_position, "size": log_batch_size}
        response = self.run_method(
            endpoint=f"/batches/{session_id}/log",
            data=log_params,
            headers=self.extra_headers,
        )
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning(
                "Got status code %d for session %s", err.response.status_code, session_id
            )
            raise AirflowException(
                f"Could not fetch the logs for batch with session id: {session_id}. "
                f"Message: {err.response.text}"
            )
        return response.json()

    def dump_batch_logs(self, session_id: int | str) -> None:
        """
        Dump the session logs for a specified batch.

        :param session_id: identifier of the batch sessions
        :return: response body
        """
        self.log.info("Fetching the logs for batch session with id: %s", session_id)
        log_start_line = 0
        log_total_lines = 0
        log_batch_size = 100

        while log_start_line <= log_total_lines:
            # Livy log  endpoint is paginated.
            response = self.get_batch_logs(session_id, log_start_line, log_batch_size)
            log_total_lines = self._parse_request_response(response, "total")
            log_start_line += log_batch_size
            log_lines = self._parse_request_response(response, "log")
            for log_line in log_lines:
                self.log.info(log_line)

    @staticmethod
    def _validate_session_id(session_id: int | str) -> None:
        """
        Validate session id is a int.

        :param session_id: session id
        """
        try:
            int(session_id)
        except (TypeError, ValueError):
            raise TypeError("'session_id' must be an integer")

    @staticmethod
    def _parse_post_response(response: dict[Any, Any]) -> int | None:
        """
        Parse batch response for batch id.

        :param response: response body
        :return: session id
        """
        return response.get("id")

    @staticmethod
    def _parse_request_response(response: dict[Any, Any], parameter):
        """
        Parse batch response for batch id.

        :param response: response body
        :return: value of parameter
        """
        return response.get(parameter, [])

    @staticmethod
    def build_post_batch_body(
        file: str,
        args: Sequence[str | int | float] | None = None,
        class_name: str | None = None,
        jars: list[str] | None = None,
        py_files: list[str] | None = None,
        files: list[str] | None = None,
        archives: list[str] | None = None,
        name: str | None = None,
        driver_memory: str | None = None,
        driver_cores: int | str | None = None,
        executor_memory: str | None = None,
        executor_cores: int | None = None,
        num_executors: int | str | None = None,
        queue: str | None = None,
        proxy_user: str | None = None,
        conf: dict[Any, Any] | None = None,
    ) -> dict:
        """
        Build the post batch request body.

        .. seealso::
            For more information about the format refer to
            https://livy.apache.org/docs/latest/rest-api.html

        :param file: Path of the file containing the application to execute (required).
        :param proxy_user: User to impersonate when running the job.
        :param class_name: Application Java/Spark main class string.
        :param args: Command line arguments for the application s.
        :param jars: jars to be used in this sessions.
        :param py_files: Python files to be used in this session.
        :param files: files to be used in this session.
        :param driver_memory: Amount of memory to use for the driver process  string.
        :param driver_cores: Number of cores to use for the driver process int.
        :param executor_memory: Amount of memory to use per executor process  string.
        :param executor_cores: Number of cores to use for each executor  int.
        :param num_executors: Number of executors to launch for this session  int.
        :param archives: Archives to be used in this session.
        :param queue: The name of the YARN queue to which submitted string.
        :param name: The name of this session string.
        :param conf: Spark configuration properties.
        :return: request body
        """
        body: dict[str, Any] = {"file": file}

        if proxy_user:
            body["proxyUser"] = proxy_user
        if class_name:
            body["className"] = class_name
        if args and LivyHook._validate_list_of_stringables(args):
            body["args"] = [str(val) for val in args]
        if jars and LivyHook._validate_list_of_stringables(jars):
            body["jars"] = jars
        if py_files and LivyHook._validate_list_of_stringables(py_files):
            body["pyFiles"] = py_files
        if files and LivyHook._validate_list_of_stringables(files):
            body["files"] = files
        if driver_memory and LivyHook._validate_size_format(driver_memory):
            body["driverMemory"] = driver_memory
        if driver_cores:
            body["driverCores"] = driver_cores
        if executor_memory and LivyHook._validate_size_format(executor_memory):
            body["executorMemory"] = executor_memory
        if executor_cores:
            body["executorCores"] = executor_cores
        if num_executors:
            body["numExecutors"] = num_executors
        if archives and LivyHook._validate_list_of_stringables(archives):
            body["archives"] = archives
        if queue:
            body["queue"] = queue
        if name:
            body["name"] = name
        if conf and LivyHook._validate_extra_conf(conf):
            body["conf"] = conf

        return body

    @staticmethod
    def _validate_size_format(size: str) -> bool:
        """
        Validate size format.

        :param size: size value
        :return: true if valid format
        """
        if size and not (
            isinstance(size, str) and re.fullmatch(r"\d+[kmgt]b?", size, re.IGNORECASE)
        ):
            raise ValueError(f"Invalid java size format for string'{size}'")
        return True

    @staticmethod
    def _validate_list_of_stringables(vals: Sequence[str | int | float]) -> bool:
        """
        Check the values in the provided list can be converted to strings.

        :param vals: list to validate
        :return: true if valid
        """
        if (
            vals is None
            or not isinstance(vals, (tuple, list))
            or not all(isinstance(val, (str, int, float)) for val in vals)
        ):
            raise ValueError("List of strings expected")
        return True

    @staticmethod
    def _validate_extra_conf(conf: dict[Any, Any]) -> bool:
        """
        Check configuration values are either strings or ints.

        :param conf: configuration variable
        :return: true if valid
        """
        if conf:
            if not isinstance(conf, dict):
                raise ValueError("'conf' argument must be a dict")
            if not all(isinstance(v, (str, int)) and v != "" for v in conf.values()):
                raise ValueError("'conf' values must be either strings or ints")
        return True


class LivyAsyncHook(HttpAsyncHook, LoggingMixin):
    """
    Hook for Apache Livy through the REST API asynchronously.

    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :param extra_options: A dictionary of options passed to Livy.
    :param extra_headers: A dictionary of headers passed to the HTTP request to livy.

    .. seealso::
        For more details refer to the Apache Livy API reference:
        https://livy.apache.org/docs/latest/rest-api.html
    """

    TERMINAL_STATES = {
        BatchState.SUCCESS,
        BatchState.DEAD,
        BatchState.KILLED,
        BatchState.ERROR,
    }

    _def_headers = {"Content-Type": "application/json", "Accept": "application/json"}

    conn_name_attr = "livy_conn_id"
    default_conn_name = "livy_default"
    conn_type = "livy"
    hook_name = "Apache Livy"

    def __init__(
        self,
        livy_conn_id: str = default_conn_name,
        extra_options: dict[str, Any] | None = None,
        extra_headers: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(http_conn_id=livy_conn_id)
        self.extra_headers = extra_headers or {}
        self.extra_options = extra_options or {}

    async def _do_api_call_async(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> Any:
        """
        Perform an asynchronous HTTP request call.

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
        """
        extra_options = extra_options or {}

        # headers may be passed through directly or in the "extra" field in the connection
        # definition
        _headers = {}
        auth = None

        if self.http_conn_id:
            conn = await sync_to_async(self.get_connection)(self.http_conn_id)

            self.base_url = self._generate_base_url(conn)
            if conn.login:
                auth = self.auth_type(conn.login, conn.password)
            if conn.extra:
                try:
                    _headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning(
                        "Connection to %s has invalid extra field.", conn.host
                    )
        if headers:
            _headers.update(headers)

        if (
            self.base_url
            and not self.base_url.endswith("/")
            and endpoint
            and not endpoint.startswith("/")
        ):
            url = self.base_url + "/" + endpoint
        else:
            url = (self.base_url or "") + (endpoint or "")

        async with aiohttp.ClientSession() as session:
            if self.method == "GET":
                request_func = session.get
            elif self.method == "POST":
                request_func = session.post
            elif self.method == "PATCH":
                request_func = session.patch
            else:
                return {
                    "Response": f"Unexpected HTTP Method: {self.method}",
                    "status": "error",
                }

            for attempt_num in range(1, 1 + self.retry_limit):
                response = await request_func(
                    url,
                    json=data if self.method in ("POST", "PATCH") else None,
                    params=data if self.method == "GET" else None,
                    headers=headers,
                    auth=auth,
                    **extra_options,
                )
                try:
                    response.raise_for_status()
                    return await response.json()
                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt_num,
                        self.retry_limit,
                        url,
                    )
                    if (
                        not self._retryable_error_async(e)
                        or attempt_num == self.retry_limit
                    ):
                        self.log.exception("HTTP error, status code: %s", e.status)
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        return {
                            "Response": {e.message},
                            "Status Code": {e.status},
                            "status": "error",
                        }

                await asyncio.sleep(self.retry_delay)

    def _generate_base_url(self, conn: Connection) -> str:
        if conn.host and "://" in conn.host:
            base_url: str = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            host = conn.host if conn.host else ""
            base_url = f"{schema}://{host}"
        if conn.port:
            base_url = f"{base_url}:{conn.port}"
        return base_url

    async def run_method(
        self,
        endpoint: str,
        method: str = "GET",
        data: Any | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        """
        Wrap HttpAsyncHook; allows to change method on the same HttpAsyncHook.

        :param method: http method
        :param endpoint: endpoint
        :param data: request payload
        :param headers: headers
        :return: http response
        """
        if method not in ("GET", "POST", "PUT", "DELETE", "HEAD"):
            return {"status": "error", "response": f"Invalid http method {method}"}

        back_method = self.method
        self.method = method
        try:
            result = await self._do_api_call_async(
                endpoint, data, headers, self.extra_options
            )
        finally:
            self.method = back_method
        return {"status": "success", "response": result}

    async def get_batch_state(self, session_id: int | str) -> Any:
        """
        Fetch the state of the specified batch asynchronously.

        :param session_id: identifier of the batch sessions
        :return: batch state
        """
        self._validate_session_id(session_id)
        self.log.info("Fetching info for batch session %s", session_id)
        result = await self.run_method(endpoint=f"/batches/{session_id}/state")
        if result["status"] == "error":
            self.log.info(result)
            return {"batch_state": "error", "response": result, "status": "error"}

        if "state" not in result["response"]:
            self.log.info(
                "batch_state: error with as it is unable to get state for batch with id: %s",
                session_id,
            )
            return {
                "batch_state": "error",
                "response": f"Unable to get state for batch with id: {session_id}",
                "status": "error",
            }

        self.log.info("Successfully fetched the batch state.")
        return {
            "batch_state": BatchState(result["response"]["state"]),
            "response": "successfully fetched the batch state.",
            "status": "success",
        }

    async def get_batch_logs(
        self, session_id: int | str, log_start_position: int, log_batch_size: int
    ) -> Any:
        """
        Get the session logs for a specified batch asynchronously.

        :param session_id: identifier of the batch sessions
        :param log_start_position: Position from where to pull the logs
        :param log_batch_size: Number of lines to pull in one batch
        :return: response body
        """
        self._validate_session_id(session_id)
        log_params = {"from": log_start_position, "size": log_batch_size}
        result = await self.run_method(
            endpoint=f"/batches/{session_id}/log", data=log_params
        )
        if result["status"] == "error":
            self.log.info(result)
            return {"response": result["response"], "status": "error"}
        return {"response": result["response"], "status": "success"}

    async def dump_batch_logs(self, session_id: int | str) -> Any:
        """
        Dump the session logs for a specified batch asynchronously.

        :param session_id: identifier of the batch sessions
        :return: response body
        """
        self.log.info("Fetching the logs for batch session with id: %s", session_id)
        log_start_line = 0
        log_total_lines = 0
        log_batch_size = 100

        while log_start_line <= log_total_lines:
            # Livy log endpoint is paginated.
            result = await self.get_batch_logs(session_id, log_start_line, log_batch_size)
            if result["status"] == "success":
                log_start_line += log_batch_size
                log_lines = self._parse_request_response(result["response"], "log")
                for log_line in log_lines:
                    self.log.info(log_line)
                return log_lines
            else:
                self.log.info(result["response"])
                return result["response"]

    @staticmethod
    def _validate_session_id(session_id: int | str) -> None:
        """
        Validate session id is a int.

        :param session_id: session id
        """
        try:
            int(session_id)
        except (TypeError, ValueError):
            raise TypeError("'session_id' must be an integer")

    @staticmethod
    def _parse_post_response(response: dict[Any, Any]) -> Any:
        """
        Parse batch response for batch id.

        :param response: response body
        :return: session id
        """
        return response.get("id")

    @staticmethod
    def _parse_request_response(response: dict[Any, Any], parameter: Any) -> Any:
        """
        Parse batch response for batch id.

        :param response: response body
        :return: value of parameter
        """
        return response.get(parameter)

    @staticmethod
    def build_post_batch_body(
        file: str,
        args: Sequence[str | int | float] | None = None,
        class_name: str | None = None,
        jars: list[str] | None = None,
        py_files: list[str] | None = None,
        files: list[str] | None = None,
        archives: list[str] | None = None,
        name: str | None = None,
        driver_memory: str | None = None,
        driver_cores: int | str | None = None,
        executor_memory: str | None = None,
        executor_cores: int | None = None,
        num_executors: int | str | None = None,
        queue: str | None = None,
        proxy_user: str | None = None,
        conf: dict[Any, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Build the post batch request body.

        :param file: Path of the file containing the application to execute (required).
        :param proxy_user: User to impersonate when running the job.
        :param class_name: Application Java/Spark main class string.
        :param args: Command line arguments for the application s.
        :param jars: jars to be used in this sessions.
        :param py_files: Python files to be used in this session.
        :param files: files to be used in this session.
        :param driver_memory: Amount of memory to use for the driver process  string.
        :param driver_cores: Number of cores to use for the driver process int.
        :param executor_memory: Amount of memory to use per executor process  string.
        :param executor_cores: Number of cores to use for each executor  int.
        :param num_executors: Number of executors to launch for this session  int.
        :param archives: Archives to be used in this session.
        :param queue: The name of the YARN queue to which submitted string.
        :param name: The name of this session string.
        :param conf: Spark configuration properties.
        :return: request body
        """
        body: dict[str, Any] = {"file": file}

        if proxy_user:
            body["proxyUser"] = proxy_user
        if class_name:
            body["className"] = class_name
        if args and LivyAsyncHook._validate_list_of_stringables(args):
            body["args"] = [str(val) for val in args]
        if jars and LivyAsyncHook._validate_list_of_stringables(jars):
            body["jars"] = jars
        if py_files and LivyAsyncHook._validate_list_of_stringables(py_files):
            body["pyFiles"] = py_files
        if files and LivyAsyncHook._validate_list_of_stringables(files):
            body["files"] = files
        if driver_memory and LivyAsyncHook._validate_size_format(driver_memory):
            body["driverMemory"] = driver_memory
        if driver_cores:
            body["driverCores"] = driver_cores
        if executor_memory and LivyAsyncHook._validate_size_format(executor_memory):
            body["executorMemory"] = executor_memory
        if executor_cores:
            body["executorCores"] = executor_cores
        if num_executors:
            body["numExecutors"] = num_executors
        if archives and LivyAsyncHook._validate_list_of_stringables(archives):
            body["archives"] = archives
        if queue:
            body["queue"] = queue
        if name:
            body["name"] = name
        if conf and LivyAsyncHook._validate_extra_conf(conf):
            body["conf"] = conf

        return body

    @staticmethod
    def _validate_size_format(size: str) -> bool:
        """
        Validate size format.

        :param size: size value
        :return: true if valid format
        """
        if size and not (
            isinstance(size, str) and re.fullmatch(r"\d+[kmgt]b?", size, re.IGNORECASE)
        ):
            raise ValueError(f"Invalid java size format for string'{size}'")
        return True

    @staticmethod
    def _validate_list_of_stringables(vals: Sequence[str | int | float]) -> bool:
        """
        Check the values in the provided list can be converted to strings.

        :param vals: list to validate
        :return: true if valid
        """
        if (
            vals is None
            or not isinstance(vals, (tuple, list))
            or not all(isinstance(val, (str, int, float)) for val in vals)
        ):
            raise ValueError("List of strings expected")
        return True

    @staticmethod
    def _validate_extra_conf(conf: dict[Any, Any]) -> bool:
        """
        Check configuration values are either strings or ints.

        :param conf: configuration variable
        :return: true if valid
        """
        if conf:
            if not isinstance(conf, dict):
                raise ValueError("'conf' argument must be a dict")
            if not all(isinstance(v, (str, int)) and v != "" for v in conf.values()):
                raise ValueError("'conf' values must be either strings or ints")
        return True
