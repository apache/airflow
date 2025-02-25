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
"""This module contains AWS MWAA hook."""

from __future__ import annotations

from typing import Any

import requests
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class MwaaHook(AwsBaseHook):
    """
    Interact with AWS Manager Workflows for Apache Airflow.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("mwaa") <MWAA.Client>`

    If your IAM policy doesn't have `airflow:InvokeRestApi` permission or if you reach throttling capacity, the
    hook will use a session token to make the requests. Learn more here:
    https://docs.aws.amazon.com/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html#granting-access-MWAA-Enhanced-REST-API

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "mwaa"
        super().__init__(*args, **kwargs)
        self._env_to_session_conn_map: dict[str, dict[str, Any]] = {}

    def invoke_rest_api(
        self,
        env_name: str,
        path: str,
        method: str,
        body: dict | None = None,
        query_params: dict | None = None,
    ) -> dict:
        """
        Invoke the REST API on the Airflow webserver with the specified inputs.

        .. seealso::
            - :external+boto3:py:meth:`MWAA.Client.invoke_rest_api`

        :param env_name: name of the MWAA environment
        :param path: Apache Airflow REST API endpoint path to be called
        :param method: HTTP method used for making Airflow REST API calls: 'GET'|'PUT'|'POST'|'PATCH'|'DELETE'
        :param body: Request body for the Apache Airflow REST API call
        :param query_params: Query parameters to be included in the Apache Airflow REST API call
        """
        # Filter out keys with None values because Airflow REST API doesn't accept requests otherwise
        body = {k: v for k, v in body.items() if v is not None} if body else {}
        query_params = query_params or {}
        api_kwargs = {
            "Name": env_name,
            "Path": path,
            "Method": method,
            "Body": body,
            "QueryParameters": query_params,
        }
        try:
            response = self.conn.invoke_rest_api(**api_kwargs)
            # ResponseMetadata is removed because it contains data that is either very unlikely to be useful
            # in XComs and logs, or redundant given the data already included in the response
            response.pop("ResponseMetadata", None)
            return response

        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDeniedException":
                self.log.info(
                    "Access Denied, possibly due to missing airflow:InvokeRestApi in IAM policy. "
                    "Trying again with session token..."
                )
                return self._invoke_rest_api_using_session_token(env_name, path, method, body, query_params)
            else:
                to_log = e.response
                # ResponseMetadata is removed because it contains data that is either very unlikely to be
                # useful in XComs and logs, or redundant given the data already included in the response
                to_log.pop("ResponseMetadata", None)
                self.log.error(to_log)
                raise e

    def _invoke_rest_api_using_session_token(
        self,
        env_name: str,
        path: str,
        method: str,
        body: dict | None = None,
        query_params: dict | None = None,
    ) -> dict:
        def try_request():
            conn_info = self._env_to_session_conn_map[env_name]
            response = conn_info["session"].request(
                method=method,
                url=f"https://{conn_info['hostname']}/api/v1{path}",
                params=query_params,
                json=body,
                timeout=10,
            )
            response.raise_for_status()
            return response

        try:
            response = try_request()
        except (requests.exceptions.HTTPError, KeyError):
            self._update_session_conn(env_name)
            response = try_request()

        return {
            "RestApiStatusCode": response.status_code,
            "RestApiResponse": response.json(),
        }

    # Based on: https://docs.aws.amazon.com/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html#create-web-server-session-token
    def _update_session_conn(self, env_name: str):
        create_token_response = self.conn.create_web_login_token(Name=env_name)
        web_server_hostname = create_token_response["WebServerHostname"]
        web_token = create_token_response["WebToken"]

        login_url = f"https://{web_server_hostname}/aws_mwaa/login"
        login_payload = {"token": web_token}
        session = requests.Session()
        login_response = session.post(login_url, data=login_payload, timeout=10)
        login_response.raise_for_status()

        self._env_to_session_conn_map[env_name] = {"session": session, "hostname": web_server_hostname}
