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

import base64
from typing import List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.providers.docker.credentials.base import BaseDockerCredentialHelper, DockerLoginCredentials


class EcrDockerCredentialHelper(BaseDockerCredentialHelper):
    """
    Authenticate into Amazon ECR (Elastic Container Registry).

    This helper ignore all connection information such as password and registry host otherwise use
    credentials retrieved from AWS API.

    .. seealso::
        - `ECR Registry Auth <https://docs.aws.amazon.com/AmazonECR/latest/userguide/registry_auth.html>`_
        - :ref:`howto/connection:aws:configuring-the-connection`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used.
    :param region_name: AWS Region Name.
    :param registry_ids: A list of Amazon Web Services account IDs that are associated with the registries
        for which to get AuthorizationData objects.
        If you do not specify a registry, the default registry is assumed.
    """

    def __init__(
        self,
        *,
        aws_conn_id: Optional[str] = "aws_default",
        region_name: Optional[str] = None,
        registry_ids: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        if isinstance(registry_ids, str):
            registry_ids = [registry_ids]
        self.registry_ids = registry_ids

    def get_credentials(self) -> Optional[Sequence[DockerLoginCredentials]]:
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        except ImportError:
            raise AirflowException(
                "apache-airflow-providers-amazon not installed, run: "
                "pip install 'apache-airflow-providers-docker[amazon]'."
            )

        aws_hook = AwsBaseHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name, client_type="ecr")
        if self.registry_ids:
            response = aws_hook.conn.get_authorization_token(registryIds=self.registry_ids)
        else:
            response = aws_hook.conn.get_authorization_token()

        creds = []
        for auth_data in response["authorizationData"]:
            username, password = base64.b64decode(auth_data["authorizationToken"]).decode("utf-8").split(":")
            registry: str = auth_data['proxyEndpoint']
            creds.append(
                DockerLoginCredentials(
                    username=username,
                    password=password,
                    # https://github.com/docker/docker-py/issues/2256#issuecomment-824940506
                    registry=registry.replace("https://", ""),
                    reauth=True,
                )
            )
            self.log.info("Credentials to Amazon ECR %r expires at %s.", registry, auth_data['expiresAt'])

        return creds
