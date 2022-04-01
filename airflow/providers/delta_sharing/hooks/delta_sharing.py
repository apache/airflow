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
import copy
import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from requests import exceptions as requests_exceptions
from tenacity import RetryError, Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property


USER_AGENT_HEADER = {'user-agent': f'airflow-{__version__}'}


# TODO: create separate data classes for protocol/metadata/files?
@dataclass
class DeltaSharingQueryResult:
    """Data class to hold results from querying a Delta Sharing table"""

    version: str
    protocol: Dict[str, Any]
    metadata: Dict[str, Any]
    files: List[Dict[str, Any]]


class DeltaSharingHook(BaseHook):
    """
    Hook to interaction with Delta Sharing endpoint.

    :param delta_sharing_conn_id: Reference to the
        :ref:`Delta Sharing connection <howto/connection:delta_sharing>`.
        By default and in the common case this will be ``delta_sharing_default``. To use
        token based authentication, provide the bearer token in the password field for the
        connection and put the base URL in the ``host`` field.
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param retry_limit: Amount of times retry if the Delta Sharing backend is
        unreachable. Its value must be greater than or equal to 1.
    :param retry_delay: Number of seconds for initial wait between retries (it
            might be a floating point number).
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    """

    conn_name_attr = 'delta_sharing_conn_id'
    default_conn_name = 'delta_sharing_default'
    hook_name = 'DeltaSharing'
    conn_type = 'delta_sharing'

    def __init__(
        self,
        delta_sharing_conn_id: str = default_conn_name,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 2.0,
        retry_args: Optional[Dict[Any, Any]] = None,
    ) -> None:
        super().__init__()
        self.delta_sharing_conn_id = delta_sharing_conn_id
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError('Retry limit must be greater than or equal to 1')
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

        def my_after_func(retry_state):
            self._log_request_error(retry_state.attempt_number, retry_state.outcome)

        if retry_args:
            self.retry_args = copy.copy(retry_args)
            self.retry_args['retry'] = retry_if_exception(self._retryable_error)
            self.retry_args['after'] = my_after_func
        else:
            self.retry_args = dict(
                stop=stop_after_attempt(self.retry_limit),
                wait=wait_exponential(min=self.retry_delay, max=(2**retry_limit)),
                retry=retry_if_exception(self._retryable_error),
                after=my_after_func,
            )

    @cached_property
    def delta_sharing_conn(self) -> Connection:
        return self.get_connection(self.delta_sharing_conn_id)

    def get_conn(self) -> Connection:
        return self.delta_sharing_conn

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error(
            'Attempt %s API Request to Delta Sharing server failed with reason: %s', attempt_num, error
        )

    def _get_retry_object(self) -> Retrying:
        """
        Instantiates a retry object
        :return: instance of Retrying class
        """
        return Retrying(**self.retry_args)

    @cached_property
    def delta_sharing_endpoint(self) -> str:
        endpoint = self.delta_sharing_conn.host
        if endpoint is None:
            raise AirflowException("Please provide Delta Sharing endpoint URL")
        if not endpoint.endswith("/"):
            endpoint += "/"

        return endpoint

    def _do_api_call(self, endpoint: str, json: Optional[Dict[str, Any]] = None, http_method='GET'):
        url = self.delta_sharing_endpoint + endpoint
        headers = USER_AGENT_HEADER.copy()
        token = self.delta_sharing_conn.password
        if token is None:
            raise AirflowException("Please provide Delta Sharing bearer token as 'password' configuration")
        headers['Authorization'] = f'Bearer {token}'
        self.log.info("url: '%s', headers: %s", url, headers)

        request_func: Any
        if http_method == 'GET':
            request_func = requests.get
        elif http_method == 'HEAD':
            request_func = requests.head
        elif http_method == 'POST':
            request_func = requests.post
        else:
            raise AirflowException('Unexpected HTTP Method: ' + http_method)

        try:
            for attempt in self._get_retry_object():
                with attempt:
                    response = request_func(
                        url,
                        json=json if http_method == 'POST' else None,
                        params=json if http_method in ('GET', 'HEAD') else None,
                        headers=headers,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return response
        except RetryError:
            raise AirflowException(
                f'API requests to Delta Sharing failed {self.retry_limit} times. Giving up.'
            )
        except requests_exceptions.HTTPError as e:
            raise AirflowException(f'Response: {e.response.content}, Status Code: {e.response.status_code}')

    @staticmethod
    def _retryable_error(exception: BaseException) -> bool:
        if not isinstance(exception, requests_exceptions.RequestException):
            return False
        return isinstance(exception, (requests_exceptions.ConnectionError, requests_exceptions.Timeout)) or (
            exception.response is not None
            and (exception.response.status_code >= 500 or exception.response.status_code == 429)
        )

    def get_table_version(self, share: str, schema: str, table: str) -> str:
        """
        Returns a version of the Delta Sharing table
        :param share: name of the share in which check will be performed.
        :param schema: name of the schema (database) in which check will be performed.
        :param table: name of the table to check.
        :return: version of the given Delta Sharing table
        """
        response = self._do_api_call(
            f"shares/{share}/schemas/{schema}/tables/{table}",
            http_method='HEAD',
        )
        version = None
        for k, v in response.headers.lower_items():
            if k == 'delta-table-version':
                version = v
        if version is None:
            raise AirflowException(
                "No delta-table-version header in response from Delta Sharing server"
                f"for item {share}.{schema}.{table}"
            )

        return version

    def query_table(
        self,
        share: str,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> DeltaSharingQueryResult:
        """
        Queries a given Delta Sharing table
        :param share: name of the share in which check will be performed.
        :param schema: name of the schema (database) in which check will be performed.
        :param table: name of the table to check.
        :param predicates: optional list of strings that will be ANDed to build a filter expression.
        :param limit: optional limit on the number of records to return.
        :return:
        """
        query_body: Dict[str, Any] = {}
        if limit is not None:
            query_body["limitHint"] = limit
        if predicates is not None:
            query_body["predicateHints"] = predicates
        response = self._do_api_call(
            f"shares/{share}/schemas/{schema}/tables/{table}/query",
            json=query_body,
            http_method='POST',
        )
        version = None
        for k, v in response.headers.lower_items():
            if k == 'delta-table-version':
                version = v
        if version is None:
            raise AirflowException(
                "No delta-table-version header in response from Delta Sharing server"
                f"for item {share}.{schema}.{table}"
            )
        lines = response.text.splitlines()
        if len(lines) < 2:
            raise AirflowException(
                "Content should have at least two lines (protocol and metadata)," f" got {len(lines)}"
            )
        protocol = json.loads(lines[0])['protocol']
        meta = json.loads(lines[1])['metaData']
        files = [json.loads(line)['file'] for line in lines[2:]]

        return DeltaSharingQueryResult(version, protocol, meta, files)
