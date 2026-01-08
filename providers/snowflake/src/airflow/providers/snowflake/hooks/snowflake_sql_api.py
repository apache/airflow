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

import base64
import time
import uuid
import warnings
from datetime import timedelta
from pathlib import Path
from typing import Any

import aiohttp
import requests
from aiohttp import ClientConnectionError, ClientResponseError
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from requests.exceptions import ConnectionError, HTTPError, Timeout
from tenacity import (
    AsyncRetrying,
    Retrying,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.sql_api_generate_jwt import JWTGenerator


class SnowflakeSqlApiHook(SnowflakeHook):
    """
    A client to interact with Snowflake using SQL API and submit multiple SQL statements in a single request.

    In combination with aiohttp, make post request to submit SQL statements for execution,
    poll to check the status of the execution of a statement. Fetch query results asynchronously.

    This hook requires the snowflake_conn_id connection. This hooks mainly uses account, schema, database,
    warehouse, and an authentication mechanism from one of below:
    1. JWT Token generated from private_key_file or private_key_content. Other inputs can be defined in the connection or hook instantiation.
    2. OAuth Token generated from the refresh_token, client_id and client_secret specified in the connection

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param warehouse: name of snowflake warehouse
    :param database: name of snowflake database
    :param region: name of snowflake region
    :param role: name of snowflake role
    :param schema: name of snowflake schema
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param token_life_time: lifetime of the JWT Token in timedelta
    :param token_renewal_delta: Renewal time of the JWT Token in timedelta
    :param deferrable: Run operator in the deferrable mode.
    :param api_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` & ``tenacity.AsyncRetrying`` classes.
    """

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minute lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes

    def __init__(
        self,
        snowflake_conn_id: str,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        api_retry_args: dict[Any, Any] | None = None,  # Optional retry arguments passed to tenacity.retry
        *args: Any,
        **kwargs: Any,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta

        super().__init__(snowflake_conn_id, *args, **kwargs)
        self.private_key: Any = None

        self.retry_config = {
            "retry": retry_if_exception(self._should_retry_on_error),
            "wait": wait_exponential(multiplier=1, min=1, max=60),
            "stop": stop_after_attempt(5),
            "before_sleep": before_sleep_log(self.log, log_level=20),  # type: ignore[arg-type]
            "reraise": True,
        }
        if api_retry_args:
            self.retry_config.update(api_retry_args)

    def get_private_key(self) -> None:
        """Get the private key from snowflake connection."""
        conn = self.get_connection(self.snowflake_conn_id)

        # If private_key_file is specified in the extra json, load the contents of the file as a private key.
        # If private_key_content is specified in the extra json, use it as a private key.
        # As a next step, specify this private key in the connection configuration.
        # The connection password then becomes the passphrase for the private key.
        # If your private key is not encrypted (not recommended), then leave the password empty.

        private_key_file = conn.extra_dejson.get(
            "extra__snowflake__private_key_file"
        ) or conn.extra_dejson.get("private_key_file")
        private_key_content = conn.extra_dejson.get(
            "extra__snowflake__private_key_content"
        ) or conn.extra_dejson.get("private_key_content")

        private_key_pem = None
        if private_key_content and private_key_file:
            raise AirflowException(
                "The private_key_file and private_key_content extra fields are mutually exclusive. "
                "Please remove one."
            )
        if private_key_file:
            private_key_pem = Path(private_key_file).read_bytes()
        elif private_key_content:
            private_key_pem = base64.b64decode(private_key_content)

        if private_key_pem:
            passphrase = None
            if conn.password:
                passphrase = conn.password.strip().encode()

            self.private_key = serialization.load_pem_private_key(
                private_key_pem, password=passphrase, backend=default_backend()
            )

    def execute_query(
        self, sql: str, statement_count: int, query_tag: str = "", bindings: dict[str, Any] | None = None
    ) -> list[str]:
        """
        Run the query in Snowflake using SnowflakeSQL API by making API request.

        :param sql: the sql string to be executed with possibly multiple statements
        :param statement_count: set the MULTI_STATEMENT_COUNT field to the number of SQL statements
         in the request
        :param query_tag: (Optional) Query tag that you want to associate with the SQL statement.
            For details, see https://docs.snowflake.com/en/sql-reference/parameters.html#label-query-tag
            parameter.
        :param bindings: (Optional) Values of bind variables in the SQL statement.
            When executing the statement, Snowflake replaces placeholders (? and :name) in
            the statement with these specified values.
        """
        self.query_ids = []
        conn_config = self._get_conn_params()

        req_id = uuid.uuid4()
        url = f"{self.account_identifier}.snowflakecomputing.com/api/v2/statements"
        params: dict[str, Any] | None = {"requestId": str(req_id), "async": True, "pageSize": 10}
        headers = self.get_headers()
        sql_is_multi_stmt = ";" in sql.strip()
        if not isinstance(bindings, dict) and bindings is not None:
            raise AirflowException("Bindings should be a dictionary or None.")
        if bindings and sql_is_multi_stmt:
            self.log.warning(
                "Bindings are not supported for multi-statement queries. Bindings will be ignored."
            )
        bindings = bindings or {}
        data = {
            "statement": sql,
            "resultSetMetaData": {"format": "json"},
            # If database, schema, warehouse, role parameters have been provided set them accordingly
            # If either of them has been not (Parent class initializes them to None in that case)
            # set them to what in the Airflow connection configuration
            "database": self.database or conn_config["database"],
            "schema": self.schema or conn_config["schema"],
            "warehouse": self.warehouse or conn_config["warehouse"],
            "role": self.role or conn_config["role"],
            "bindings": bindings,
            "parameters": {
                "MULTI_STATEMENT_COUNT": statement_count,
                "query_tag": query_tag,
            },
        }

        _, json_response = self._make_api_call_with_retries("POST", url, headers, params, data)
        self.log.info("Snowflake SQL POST API response: %s", json_response)
        if "statementHandles" in json_response:
            self.query_ids = json_response["statementHandles"]
        elif "statementHandle" in json_response:
            self.query_ids.append(json_response["statementHandle"])
        else:
            raise AirflowException("No statementHandle/statementHandles present in response")
        return self.query_ids

    def get_headers(self) -> dict[str, Any]:
        """Form auth headers based on either OAuth token or JWT token from private key."""
        conn_config = self._get_conn_params()

        # Use OAuth if refresh_token and client_id and client_secret are provided
        if all(
            [conn_config.get("refresh_token"), conn_config.get("client_id"), conn_config.get("client_secret")]
        ):
            oauth_token = self.get_oauth_token(conn_config=conn_config)
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {oauth_token}",
                "Accept": "application/json",
                "User-Agent": "snowflakeSQLAPI/1.0",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
            }
            return headers

        # Alternatively, get the JWT token from the connection details and the private key
        if not self.private_key:
            self.get_private_key()

        token = JWTGenerator(
            conn_config["account"],  # type: ignore[arg-type]
            conn_config["user"],  # type: ignore[arg-type]
            private_key=self.private_key,
            lifetime=self.token_life_time,
            renewal_delay=self.token_renewal_delta,
        ).get_token()

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "snowflakeSQLAPI/1.0",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        return headers

    def get_oauth_token(
        self,
        conn_config: dict[str, Any] | None = None,
        token_endpoint: str | None = None,
        grant_type: str = "refresh_token",
    ) -> str:
        """Generate temporary OAuth access token using refresh token in connection details."""
        warnings.warn(
            "This method is deprecated. Please use `get_oauth_token` method from `SnowflakeHook` instead. ",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return super().get_oauth_token(
            conn_config=conn_config, token_endpoint=token_endpoint, grant_type=grant_type
        )

    def get_request_url_header_params(
        self, query_id: str, url_suffix: str | None = None
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        """
        Build the request header Url with account name identifier and query id from the connection params.

        :param query_id: statement handles query ids for the individual statements.
        :param url_suffix: Optional path suffix to append to the URL. Must start with '/', e.g. '/cancel' or '/result'.
        """
        req_id = uuid.uuid4()
        header = self.get_headers()
        params = {"requestId": str(req_id)}
        url = f"{self.account_identifier}.snowflakecomputing.com/api/v2/statements/{query_id}"
        if url_suffix:
            url += url_suffix
        return header, params, url

    def check_query_output(self, query_ids: list[str]) -> None:
        """
        Make HTTP request to snowflake SQL API based on the provided query ids and log the response.

        :param query_ids: statement handles query id for the individual statements.
        """
        for query_id in query_ids:
            header, params, url = self.get_request_url_header_params(query_id)
            _, response_json = self._make_api_call_with_retries(
                method="GET", url=url, headers=header, params=params
            )
            self.log.info(response_json)

    def _process_response(self, status_code, resp):
        self.log.info("Snowflake SQL GET statements status API response: %s", resp)
        if status_code == 202:
            return {"status": "running", "message": "Query statements are still running"}
        if status_code == 422:
            error_message = resp.get("message", "Unknown error occurred")
            error_details = []
            if code := resp.get("code"):
                error_details.append(f"Code: {code}")
            if sql_state := resp.get("sqlState"):
                error_details.append(f"SQL State: {sql_state}")
            if statement_handle := resp.get("statementHandle"):
                error_details.append(f"Statement Handle: {statement_handle}")

            if error_details:
                enhanced_message = f"{error_message} ({', '.join(error_details)})"
            else:
                enhanced_message = error_message

            return {"status": "error", "message": enhanced_message}
        if status_code == 200:
            if resp_statement_handles := resp.get("statementHandles"):
                statement_handles = resp_statement_handles
            elif resp_statement_handle := resp.get("statementHandle"):
                statement_handles = [resp_statement_handle]
            else:
                statement_handles = []
            return {
                "status": "success",
                "message": resp["message"],
                "statement_handles": statement_handles,
            }
        return {"status": "error", "message": resp["message"]}

    def get_sql_api_query_status(self, query_id: str) -> dict[str, str | list[str]]:
        """
        Based on the query id async HTTP request is made to snowflake SQL API and return response.

        :param query_id: statement handle id for the individual statements.
        """
        self.log.info("Retrieving status for query id %s", query_id)
        header, params, url = self.get_request_url_header_params(query_id)
        status_code, resp = self._make_api_call_with_retries("GET", url, header, params)
        return self._process_response(status_code, resp)

    def wait_for_query(
        self, query_id: str, raise_error: bool = False, poll_interval: int = 5, timeout: int = 60
    ) -> dict[str, str | list[str]]:
        """
        Wait for query to finish either successfully or with error.

        :param query_id: statement handle id for the individual statement.
        :param raise_error: whether to raise an error if the query failed.
        :param poll_interval: time (in seconds) between checking the query status.
        :param timeout: max time (in seconds) to wait for the query to finish before raising a TimeoutError.

        :raises RuntimeError: If the query status is 'error' and `raise_error` is True.
        :raises TimeoutError: If the query doesn't finish within the specified timeout.
        """
        start_time = time.time()

        while True:
            response = self.get_sql_api_query_status(query_id=query_id)
            self.log.debug("Query status `%s`", response["status"])

            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"Query `{query_id}` did not finish within the timeout period of {timeout} seconds."
                )

            if response["status"] != "running":
                self.log.info("Query status `%s`", response["status"])
                break

            time.sleep(poll_interval)

        if response["status"] == "error" and raise_error:
            raise RuntimeError(response["message"])

        return response

    def get_result_from_successful_sql_api_query(self, query_id: str) -> list[dict[str, Any]]:
        """
        Based on the query id HTTP requests are made to snowflake SQL API and return result data.

        :param query_id: statement handle id for the individual statement.

        :raises RuntimeError: If the query status is not 'success'.
        """
        self.log.info("Retrieving data for query id %s", query_id)
        header, params, url = self.get_request_url_header_params(query_id)
        status_code, response = self._make_api_call_with_retries("GET", url, header, params)

        if (query_status := self._process_response(status_code, response)["status"]) != "success":
            msg = f"Query must have status `success` to retrieve data; got `{query_status}`."
            raise RuntimeError(msg)

        # Below fields should always be present in response, but added some safety checks
        data = response.get("data", [])
        if not data:
            self.log.warning("No data found in the API response.")
            return []
        metadata = response.get("resultSetMetaData", {})
        col_names = [row["name"] for row in metadata.get("rowType", [])]
        if not col_names:
            self.log.warning("No column metadata found in the API response.")
            return []

        num_partitions = len(metadata.get("partitionInfo", []))
        if num_partitions > 1:
            self.log.debug("Result data is returned as multiple partitions. Will perform additional queries.")
            url += "?partition="
            for partition_no in range(1, num_partitions):  # First partition was already returned
                self.log.debug("Querying for partition no. %s", partition_no)
                _, response = self._make_api_call_with_retries("GET", url + str(partition_no), header, params)
                data.extend(response.get("data", []))

        return [dict(zip(col_names, row)) for row in data]  # Merged column names with data

    async def get_sql_api_query_status_async(self, query_id: str) -> dict[str, str | list[str]]:
        """
        Based on the query id async HTTP request is made to snowflake SQL API and return response.

        :param query_id: statement handle id for the individual statements.
        """
        self.log.info("Retrieving status for query id %s", query_id)
        header, params, url = self.get_request_url_header_params(query_id)
        status_code, resp = await self._make_api_call_with_retries_async("GET", url, header, params)
        return self._process_response(status_code, resp)

    def _cancel_sql_api_query_execution(self, query_id: str) -> dict[str, str | list[str]]:
        self.log.info("Cancelling query id %s", query_id)
        header, params, url = self.get_request_url_header_params(query_id, "/cancel")
        status_code, resp = self._make_api_call_with_retries("POST", url, header, params)
        return self._process_response(status_code, resp)

    def cancel_queries(self, query_ids: list[str]) -> None:
        for query_id in query_ids:
            self._cancel_sql_api_query_execution(query_id)

    @staticmethod
    def _should_retry_on_error(exception) -> bool:
        """
        Determine if the exception should trigger a retry based on error type and status code.

        Retries on HTTP errors 429 (Too Many Requests), 503 (Service Unavailable),
        and 504 (Gateway Timeout) as recommended by Snowflake error handling docs.
        Retries on connection errors and timeouts.

        :param exception: The exception to check
        :return: True if the request should be retried, False otherwise
        """
        if isinstance(exception, HTTPError):
            return exception.response.status_code in [429, 503, 504]
        if isinstance(exception, ClientResponseError):
            return exception.status in [429, 503, 504]
        if isinstance(
            exception,
            ConnectionError | Timeout | ClientConnectionError,
        ):
            return True
        return False

    def _make_api_call_with_retries(
        self, method: str, url: str, headers: dict, params: dict | None = None, json: dict | None = None
    ):
        """
        Make an API call to the Snowflake SQL API with retry logic for specific HTTP errors.

        Error handling implemented based on Snowflake error handling docs:
        https://docs.snowflake.com/en/developer-guide/sql-api/handling-errors

        :param method: The HTTP method to use for the API call.
        :param url: The URL for the API endpoint.
        :param headers: The headers to include in the API call.
        :param params: (Optional) The query parameters to include in the API call.
        :param json: (Optional) The data to include in the API call.
        :return: The response object from the API call.
        """
        with requests.Session() as session:
            for attempt in Retrying(**self.retry_config):  # type: ignore
                with attempt:
                    if method.upper() in ("GET", "POST"):
                        response = session.request(
                            method=method.lower(), url=url, headers=headers, params=params, json=json
                        )
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")
                    response.raise_for_status()
                    return response.status_code, response.json()

    async def _make_api_call_with_retries_async(self, method, url, headers, params=None):
        """
        Make an API call to the Snowflake SQL API asynchronously with retry logic for specific HTTP errors.

        Error handling implemented based on Snowflake error handling docs:
        https://docs.snowflake.com/en/developer-guide/sql-api/handling-errors

        :param method: The HTTP method to use for the API call. Only GET is supported as  is synchronous.
        :param url: The URL for the API endpoint.
        :param headers: The headers to include in the API call.
        :param params: (Optional) The query parameters to include in the API call.
        :return: The response object from the API call.
        """
        async with aiohttp.ClientSession(headers=headers) as session:
            async for attempt in AsyncRetrying(**self.retry_config):
                with attempt:
                    if method.upper() == "GET":
                        async with session.request(method=method.lower(), url=url, params=params) as response:
                            response.raise_for_status()
                            # Return status and json content for async processing
                            content = await response.json()
                            return response.status, content
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")
