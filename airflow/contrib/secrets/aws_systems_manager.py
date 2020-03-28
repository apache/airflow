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
"""
Objects relating to sourcing connections from AWS SSM Parameter Store
"""
from typing import Optional

import boto3
from cached_property import cached_property

from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class SystemsManagerParameterStoreBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection or Variables from AWS SSM Parameter Store

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.contrib.secrets.aws_systems_manager.SystemsManagerParameterStoreBackend
        backend_kwargs = {"connections_prefix": "/airflow/connections", "profile_name": null}

    For example, if ssm path is ``/airflow/connections/smtp_default``, this would be accessible
    if you provide ``{"connections_prefix": "/airflow/connections"}`` and request conn_id ``smtp_default``.
    And if ssm path is ``/airflow/variables/hello``, this would be accessible
    if you provide ``{"variables_prefix": "/airflow/variables"}`` and request conn_id ``hello``.
    """

    def __init__(
        self,
        connections_prefix='/airflow/connections',  # type: str
        variables_prefix='/airflow/variables',  # type: str
        profile_name=None,  # type: Optional[str]
        **kwargs
    ):
        self.connections_prefix = connections_prefix.rstrip("/")
        self.variables_prefix = variables_prefix.rstrip('/')
        self.profile_name = profile_name
        super(SystemsManagerParameterStoreBackend, self).__init__(**kwargs)

    @cached_property
    def client(self):
        """
        Create a SSM client
        """
        session = boto3.Session(profile_name=self.profile_name)
        return session.client("ssm")

    def get_conn_uri(self, conn_id):
        # type: (str) -> Optional[str]
        """
        Get param value

        :param conn_id: connection id
        :type conn_id: str
        :rtype: str
        """

        return self._get_secret(self.connections_prefix, conn_id)

    def get_variable(self, key):
        # type: (str) -> Optional[str]
        """
        Get Airflow Variable from Environment Variable

        :param key: Variable Key
        :return: Variable Value
        """
        return self._get_secret(self.variables_prefix, key)

    def _get_secret(self, path_prefix, secret_id):
        # type: (str, str) -> Optional[str]
        """
        Get secret value from Parameter Store.

        :param path_prefix: Prefix for the Path to get Secret
        :type path_prefix: str
        :param secret_id: Secret Key
        :type secret_id: str
        """
        ssm_path = self.build_path(path_prefix, secret_id)
        try:
            response = self.client.get_parameter(
                Name=ssm_path, WithDecryption=False
            )
            value = response["Parameter"]["Value"]
            return value
        except self.client.exceptions.ParameterNotFound:
            self.log.info(
                "An error occurred (ParameterNotFound) when calling the GetParameter operation: "
                "Parameter %s not found.", ssm_path
            )
            return None
