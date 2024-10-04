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

import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import aiohttp
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
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
    """

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minute lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes

    def __init__(
        self,
        snowflake_conn_id: str,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        *args: Any,
        **kwargs: Any,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        super().__init__(snowflake_conn_id, *args, **kwargs)
        self.private_key: Any = None

    @property
    def account_identifier(self) -> str:
        """Returns snowflake account identifier."""
        conn_config = self._get_conn_params
        account_identifier = f"https://{conn_config['account']}"

        if conn_config["region"]:
            account_identifier += f".{conn_config['region']}"

        return account_identifier

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
        elif private_key_file:
            private_key_pem = Path(private_key_file).read_bytes()
        elif private_key_content:
            private_key_pem = private_key_content.encode()

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
        conn_config = self._get_conn_params

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
        response = requests.post(url, json=data, headers=headers, params=params)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:  # pragma: no cover
            msg = f"Response: {e.response.content.decode()} Status Code: {e.response.status_code}"
            raise AirflowException(msg)
        json_response = response.json()
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
        conn_config = self._get_conn_params

        # Use OAuth if refresh_token and client_id and client_secret are provided
        if all(
            [conn_config.get("refresh_token"), conn_config.get("client_id"), conn_config.get("client_secret")]
        ):
            oauth_token = self.get_oauth_token()
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

    def get_oauth_token(self) -> str:
        """Generate temporary OAuth access token using refresh token in connection details."""
        conn_config = self._get_conn_params
        url = f"{self.account_identifier}.snowflakecomputing.com/oauth/token-request"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": conn_config["refresh_token"],
            "redirect_uri": conn_config.get("redirect_uri", "https://localhost.com"),
        }
        response = requests.post(
            url,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            auth=HTTPBasicAuth(conn_config["client_id"], conn_config["client_secret"]),  # type: ignore[arg-type]
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:  # pragma: no cover
            msg = f"Response: {e.response.content.decode()} Status Code: {e.response.status_code}"
            raise AirflowException(msg)
        return response.json()["access_token"]

    def get_request_url_header_params(self, query_id: str) -> tuple[dict[str, Any], dict[str, Any], str]:
        """
        Build the request header Url with account name identifier and query id from the connection params.

        :param query_id: statement handles query ids for the individual statements.
        """
        req_id = uuid.uuid4()
        header = self.get_headers()
        params = {"requestId": str(req_id)}
        url = f"{self.account_identifier}.snowflakecomputing.com/api/v2/statements/{query_id}"
        return header, params, url

    def check_query_output(self, query_ids: list[str]) -> None:
        """
        Make HTTP request to snowflake SQL API based on the provided query ids and log the response.

        :param query_ids: statement handles query id for the individual statements.
        """
        for query_id in query_ids:
            header, params, url = self.get_request_url_header_params(query_id)
            try:
                response = requests.get(url, headers=header, params=params)
                response.raise_for_status()
                self.log.info(response.json())
            except requests.exceptions.HTTPError as e:
                msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
                raise AirflowException(msg)

    def _process_response(self, status_code, resp):
        self.log.info("Snowflake SQL GET statements status API response: %s", resp)
        if status_code == 202:
            return {"status": "running", "message": "Query statements are still running"}
        elif status_code == 422:
            return {"status": "error", "message": resp["message"]}
        elif status_code == 200:
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
        else:
            return {"status": "error", "message": resp["message"]}

    def get_sql_api_query_status(self, query_id: str) -> dict[str, str | list[str]]:
        """
        Based on the query id async HTTP request is made to snowflake SQL API and return response.

        :param query_id: statement handle id for the individual statements.
        """
        self.log.info("Retrieving status for query id %s", query_id)
        header, params, url = self.get_request_url_header_params(query_id)
        response = requests.get(url, params=params, headers=header)
        status_code = response.status_code
        resp = response.json()
        return self._process_response(status_code, resp)

    async def get_sql_api_query_status_async(self, query_id: str) -> dict[str, str | list[str]]:
        """
        Based on the query id async HTTP request is made to snowflake SQL API and return response.

        :param query_id: statement handle id for the individual statements.
        """
        self.log.info("Retrieving status for query id %s", query_id)
        header, params, url = self.get_request_url_header_params(query_id)
        async with aiohttp.ClientSession(headers=header) as session, session.get(
            url, params=params
        ) as response:
            status_code = response.status
            resp = await response.json()
            return self._process_response(status_code, resp)
