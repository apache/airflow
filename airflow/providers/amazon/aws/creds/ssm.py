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

from airflow.configuration import conf
from airflow.creds import CONN_ENV_PREFIX, BaseCredsBackend
from airflow.models import Connection


class AwsSsmCredsBackend(BaseCredsBackend):
    """
    Retrieves Connection object from AWS SSM Parameter Store

    Configurable via airflow config like so:

    .. code-block:: ini

        [aws_ssm_creds]
        ssm_prefix = /airflow
        profile_name = default

    For example, if ssm path is ``/airflow/AIRFLOW_CONN_SMTP_DEFAULT``, this would be accessible if you
    provide ``ssm_prefix = /airflow`` and conn_id ``smtp_default``.

    """

    CONF_SECTION = "aws_ssm_creds"
    CONF_KEY_SSM_PREFIX = "ssm_prefix"
    DEFAULT_PREFIX = "/airflow"
    CONF_KEY_PROFILE_NAME = "profile_name"

    def __init__(self, *args, **kwargs):
        pass

    @property
    def ssm_prefix(self) -> str:
        """
        Gets ssm prefix from conf.

        Ensures that there is no trailing slash.

        :return:
        """
        ssm_prefix = conf.get(
            section=self.CONF_SECTION, key=self.CONF_KEY_SSM_PREFIX, fallback=self.DEFAULT_PREFIX
        )
        return ssm_prefix.rstrip("/")

    @property
    def aws_profile_name(self) -> Optional[str]:
        """
        Gets AWS profile to use from conf.

        :return:
        """
        profile_name = conf.get(
            section=self.CONF_SECTION, key=self.CONF_KEY_PROFILE_NAME, fallback=None
        )
        return profile_name or None

    def build_ssm_path(self, conn_id: str):
        """
        Given conn_id, build SSM path.

        Assumes connection params use same naming convention as env vars, but may have arbitrary prefix.

        :param conn_id:
        :return:
        """
        param_name = (CONN_ENV_PREFIX + conn_id).upper()
        param_path = self.ssm_prefix + "/" + param_name
        return param_path

    def get_conn_uri(self, conn_id):
        """
        Get param value

        :param conn_id:
        :return:
        """
        session = boto3.Session(profile_name=self.aws_profile_name)
        client = session.client("ssm")
        response = client.get_parameter(
            Name=self.build_ssm_path(conn_id=conn_id), WithDecryption=True
        )
        value = response["Parameter"]["Value"]
        return value

    def get_connections(self, conn_id) -> List[Connection]:
        """
        Create connection object.

        :param conn_id:
        :return:
        """
        conn_uri = self.get_conn_uri(conn_id=conn_id)
        conn = Connection(conn_id=conn_id, uri=conn_uri)
        return [conn]
