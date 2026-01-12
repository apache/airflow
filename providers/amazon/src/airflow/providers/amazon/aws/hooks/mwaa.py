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

import warnings
from typing import Literal

import requests
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class MwaaHook(AwsBaseHook):
    """
    Interact with AWS Managed Workflows for Apache Airflow.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("mwaa") <MWAA.Client>`

    If your IAM policy doesn't have `airflow:InvokeRestApi` permission, the hook will use a fallback method
    that uses the AWS credential to generate a local web login token for the Airflow Web UI and then directly
    make requests to the Airflow API. This fallback method can be set as the default (and only) method used by
    setting `generate_local_token` to True.  Learn more here:
    https://docs.aws.amazon.com/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html#granting-access-MWAA-Enhanced-REST-API

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "mwaa"
        super().__init__(*args, **kwargs)

    def invoke_rest_api(
        self,
        env_name: str,
        path: str,
        method: str,
        body: dict | None = None,
        query_params: dict | None = None,
        generate_local_token: bool = False,
        airflow_version: Literal[2, 3] | None = None,
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
        :param generate_local_token: If True, only the local web token method is used without trying boto's
            `invoke_rest_api` first. If False, the local web token method is used as a fallback after trying
            boto's `invoke_rest_api`
        :param airflow_version: The Airflow major version the MWAA environment runs.
            This parameter is only used if the local web token method is used to call Airflow API.
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

        if generate_local_token:
            return self._invoke_rest_api_using_local_session_token(airflow_version, **api_kwargs)

        try:
            response = self.conn.invoke_rest_api(**api_kwargs)
            # ResponseMetadata is removed because it contains data that is either very unlikely to be useful
            # in XComs and logs, or redundant given the data already included in the response
            response.pop("ResponseMetadata", None)
            return response

        except ClientError as e:
            if (
                e.response["Error"]["Code"] == "AccessDeniedException"
                and "Airflow role" in e.response["Error"]["Message"]
            ):
                self.log.info(
                    "Access Denied due to missing airflow:InvokeRestApi in IAM policy. Trying again by generating local token..."
                )
                return self._invoke_rest_api_using_local_session_token(airflow_version, **api_kwargs)
            to_log = e.response
            # ResponseMetadata is removed because it contains data that is either very unlikely to be
            # useful in XComs and logs, or redundant given the data already included in the response
            to_log.pop("ResponseMetadata", None)
            self.log.error(to_log)
            raise

    def _invoke_rest_api_using_local_session_token(
        self,
        airflow_version: Literal[2, 3] | None = None,
        **api_kwargs,
    ) -> dict:
        if not airflow_version:
            warnings.warn(
                "The parameter ``airflow_version`` in ``MwaaHook.invoke_rest_api`` is not "
                "specified and the local web token method is being used. "
                "The default Airflow version being used is 2 but this value will change in the future. "
                "To avoid any unexpected behavior, please explicitly specify the Airflow version.",
                FutureWarning,
                stacklevel=3,
            )
            airflow_version = 2

        try:
            session, hostname, login_response = self._get_session_conn(api_kwargs["Name"], airflow_version)

            headers = {}
            if airflow_version == 3:
                headers = {
                    "Authorization": f"Bearer {login_response.cookies['_token']}",
                    "Content-Type": "application/json",
                }

            api_version = "v1" if airflow_version == 2 else "v2"
            response = session.request(
                method=api_kwargs["Method"],
                url=f"https://{hostname}/api/{api_version}{api_kwargs['Path']}",
                headers=headers,
                params=api_kwargs["QueryParameters"],
                json=api_kwargs["Body"],
                timeout=10,
            )
            response.raise_for_status()

        except requests.HTTPError as e:
            self.log.error(e.response.json())
            raise

        return {
            "RestApiStatusCode": response.status_code,
            "RestApiResponse": response.json(),
        }

    # Based on: https://docs.aws.amazon.com/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html#create-web-server-session-token
    def _get_session_conn(self, env_name: str, airflow_version: Literal[2, 3]) -> tuple:
        create_token_response = self.conn.create_web_login_token(Name=env_name)
        web_server_hostname = create_token_response["WebServerHostname"]
        web_token = create_token_response["WebToken"]

        login_url = (
            f"https://{web_server_hostname}/aws_mwaa/login"
            if airflow_version == 2
            else f"https://{web_server_hostname}/pluginsv2/aws_mwaa/login"
        )
        login_payload = {"token": web_token}
        session = requests.Session()
        login_response = session.post(login_url, data=login_payload, timeout=10)
        login_response.raise_for_status()

        return session, web_server_hostname, login_response
