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

import json
import re
from enum import Enum
from typing import Any, Sequence

import requests

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.log.logging_mixin import LoggingMixin


class BatchState(Enum):
    """Batch session states"""

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
        self.auth_type = auth_type or self.auth_type

    def get_conn(self, headers: dict[str, Any] | None = None) -> Any:
        """
        Returns http session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        :return: requests session
        :rtype: requests.Session
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
        Wrapper for HttpHook, allows to change method on the same HttpHook

        :param method: http method
        :param endpoint: endpoint
        :param data: request payload
        :param headers: headers
        :param retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
        :return: http response
        :rtype: requests.Response
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

    def post_batch(self, *args: Any, **kwargs: Any) -> Any:
        """
        Perform request to submit batch

        :return: batch session id
        :rtype: int
        """
        batch_submit_body = json.dumps(self.build_post_batch_body(*args, **kwargs))

        if self.base_url is None:
            # need to init self.base_url
            self.get_conn()
        self.log.info("Submitting job %s to %s", batch_submit_body, self.base_url)

        response = self.run_method(
            method="POST", endpoint="/batches", data=batch_submit_body, headers=self.extra_headers
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
        self.log.info("Batch submitted with session id: %d", batch_id)

        return batch_id

    def get_batch(self, session_id: int | str) -> Any:
        """
        Fetch info about the specified batch

        :param session_id: identifier of the batch sessions
        :return: response body
        :rtype: dict
        """
        self._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session %d", session_id)
        response = self.run_method(endpoint=f"/batches/{session_id}", headers=self.extra_headers)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning("Got status code %d for session %d", err.response.status_code, session_id)
            raise AirflowException(
                f"Unable to fetch batch with id: {session_id}. Message: {err.response.text}"
            )

        return response.json()

    def get_batch_state(self, session_id: int | str, retry_args: dict[str, Any] | None = None) -> BatchState:
        """
        Fetch the state of the specified batch

        :param session_id: identifier of the batch sessions
        :param retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
        :return: batch state
        :rtype: BatchState
        """
        self._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session %d", session_id)
        response = self.run_method(
            endpoint=f"/batches/{session_id}/state", retry_args=retry_args, headers=self.extra_headers
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning("Got status code %d for session %d", err.response.status_code, session_id)
            raise AirflowException(
                f"Unable to fetch batch with id: {session_id}. Message: {err.response.text}"
            )

        jresp = response.json()
        if "state" not in jresp:
            raise AirflowException(f"Unable to get state for batch with id: {session_id}")
        return BatchState(jresp["state"])

    def delete_batch(self, session_id: int | str) -> Any:
        """
        Delete the specified batch

        :param session_id: identifier of the batch sessions
        :return: response body
        :rtype: dict
        """
        self._validate_session_id(session_id)

        self.log.info("Deleting batch session %d", session_id)
        response = self.run_method(
            method="DELETE", endpoint=f"/batches/{session_id}", headers=self.extra_headers
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning("Got status code %d for session %d", err.response.status_code, session_id)
            raise AirflowException(
                f"Could not kill the batch with session id: {session_id}. Message: {err.response.text}"
            )

        return response.json()

    def get_batch_logs(self, session_id: int | str, log_start_position, log_batch_size) -> Any:
        """
        Gets the session logs for a specified batch.
        :param session_id: identifier of the batch sessions
        :param log_start_position: Position from where to pull the logs
        :param log_batch_size: Number of lines to pull in one batch

        :return: response body
        :rtype: dict
        """
        self._validate_session_id(session_id)
        log_params = {"from": log_start_position, "size": log_batch_size}
        response = self.run_method(
            endpoint=f"/batches/{session_id}/log", data=log_params, headers=self.extra_headers
        )
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning("Got status code %d for session %d", err.response.status_code, session_id)
            raise AirflowException(
                f"Could not fetch the logs for batch with session id: {session_id}. "
                f"Message: {err.response.text}"
            )
        return response.json()

    def dump_batch_logs(self, session_id: int | str) -> Any:
        """
        Dumps the session logs for a specified batch

        :param session_id: identifier of the batch sessions
        :return: response body
        :rtype: dict
        """
        self.log.info("Fetching the logs for batch session with id: %d", session_id)
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
        Validate session id is a int

        :param session_id: session id
        """
        try:
            int(session_id)
        except (TypeError, ValueError):
            raise TypeError("'session_id' must be an integer")

    @staticmethod
    def _parse_post_response(response: dict[Any, Any]) -> Any:
        """
        Parse batch response for batch id

        :param response: response body
        :return: session id
        :rtype: int
        """
        return response.get("id")

    @staticmethod
    def _parse_request_response(response: dict[Any, Any], parameter) -> Any:
        """
        Parse batch response for batch id

        :param response: response body
        :return: value of parameter
        :rtype: Union[int, list]
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
    ) -> Any:
        """
        Build the post batch request body.
        For more information about the format refer to
        .. seealso:: https://livy.apache.org/docs/latest/rest-api.html
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
        :rtype: dict
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
        :rtype: bool
        """
        if size and not (isinstance(size, str) and re.match(r"^\d+[kmgt]b?$", size, re.IGNORECASE)):
            raise ValueError(f"Invalid java size format for string'{size}'")
        return True

    @staticmethod
    def _validate_list_of_stringables(vals: Sequence[str | int | float]) -> bool:
        """
        Check the values in the provided list can be converted to strings.

        :param vals: list to validate
        :return: true if valid
        :rtype: bool
        """
        if (
            vals is None
            or not isinstance(vals, (tuple, list))
            or any(1 for val in vals if not isinstance(val, (str, int, float)))
        ):
            raise ValueError("List of strings expected")
        return True

    @staticmethod
    def _validate_extra_conf(conf: dict[Any, Any]) -> bool:
        """
        Check configuration values are either strings or ints.

        :param conf: configuration variable
        :return: true if valid
        :rtype: bool
        """
        if conf:
            if not isinstance(conf, dict):
                raise ValueError("'conf' argument must be a dict")
            if any(True for k, v in conf.items() if not (v and isinstance(v, str) or isinstance(v, int))):
                raise ValueError("'conf' values must be either strings or ints")
        return True
