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

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class MwaaHook(AwsBaseHook):
    """
    Interact with AWS Manager Workflows for Apache Airflow.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("mwaa") <MWAA.Client>`

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
    ) -> dict:
        """
        Invoke the REST API on the Airflow webserver with the specified inputs.

        .. seealso::
            - :external+boto3:py:meth:`MWAA.Client.invoke_rest_api`

        :param env_name: name of the MWAA environment
        :param path: Apache Airflow REST API endpoint path to be called
        :param method: HTTP method used for making Airflow REST API calls
        :param body: Request body for the Apache Airflow REST API call
        :param query_params: Query parameters to be included in the Apache Airflow REST API call
        """
        body = body or {}
        api_kwargs = {
            "Name": env_name,
            "Path": path,
            "Method": method,
            # Filter out keys with None values because Airflow REST API doesn't accept requests otherwise
            "Body": {k: v for k, v in body.items() if v is not None},
            "QueryParameters": query_params if query_params else {},
        }
        try:
            result = self.conn.invoke_rest_api(**api_kwargs)
            # ResponseMetadata is removed because it contains data that is either very unlikely to be useful
            # in XComs and logs, or redundant given the data already included in the response
            result.pop("ResponseMetadata", None)
            return result
        except ClientError as e:
            to_log = e.response
            # ResponseMetadata and Error are removed because they contain data that is either very unlikely to
            # be useful in XComs and logs, or redundant given the data already included in the response
            to_log.pop("ResponseMetadata", None)
            to_log.pop("Error", None)
            self.log.error(to_log)
            raise e
