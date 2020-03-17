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
from typing import List, Optional

import boto3

from airflow.models import Connection
from airflow.secrets import CONN_ENV_PREFIX, BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class AwsSsmSecretsBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection object from AWS SSM Parameter Store

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.amazon.aws.secrets.ssm.AwsSsmSecretsBackend
        backend_kwargs = {"prefix": "/airflow", "profile_name": null}

    For example, if ssm path is ``/airflow/AIRFLOW_CONN_SMTP_DEFAULT``, this would be accessible if you
    provide ``{"prefix": "/airflow"}`` and request conn_id ``smtp_default``.

    """

    def __init__(self, prefix: str = '/airflow', profile_name: Optional[str] = None, **kwargs):
        self._prefix = prefix
        self.profile_name = profile_name
        super().__init__(**kwargs)

    @property
    def prefix(self) -> str:
        """
        Ensures that there is no trailing slash.
        """
        return self._prefix.rstrip("/")

    def build_ssm_path(self, conn_id: str):
        """
        Given conn_id, build SSM path.
        Assumes connection params use same naming convention as env vars, but may have arbitrary prefix.

        :param conn_id: connection id
        """
        param_name = (CONN_ENV_PREFIX + conn_id).upper()
        param_path = self.prefix + "/" + param_name
        return param_path

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Get param value

        :param conn_id: connection id
        :type conn_id: str
        """
        session = boto3.Session(profile_name=self.profile_name)
        client = session.client("ssm")
        ssm_path = self.build_ssm_path(conn_id=conn_id)
        try:
            response = client.get_parameter(
                Name=ssm_path, WithDecryption=False
            )
            value = response["Parameter"]["Value"]
            return value
        except client.exceptions.ParameterNotFound:
            self.log.info(
                "An error occurred (ParameterNotFound) when calling the GetParameter operation: "
                "Parameter %s not found.", ssm_path
            )
            return None

    def get_connections(self, conn_id: str) -> List[Connection]:
        """
        Create connection object.

        :param conn_id: connection id
        :type conn_id: str
        """
        conn_uri = self.get_conn_uri(conn_id=conn_id)
        if not conn_uri:
            return []
        conn = Connection(conn_id=conn_id, uri=conn_uri)
        return [conn]
