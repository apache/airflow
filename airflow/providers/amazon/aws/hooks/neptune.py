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

"""Interact with AWS Neptune."""
from __future__ import annotations

import time
from typing import Callable

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class NeptuneHook(AwsBaseHook):
    """
    Interact with AWS Neptune using proper client from the boto3 library.

    Hook attribute `conn` has all methods that listed in documentation

    .. seealso::
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune.html
        - https://docs.aws.amazon.com/neptune/index.html

    Additional arguments (such as ``aws_conn_id`` or ``region_name``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "neptune"
        super().__init__(*args, **kwargs)

    def get_db_cluster_state(self, db_cluster_id: str) -> str:
        """
        Get the current state of a DB cluster.

        :param db_cluster_id: The ID of the target DB cluster.
        :return: Returns the status of the DB cluster as a string (eg. "available")
        :rtype: str
        :raises AirflowNotFoundException: If the DB cluster does not exist.
        """
        try:
            response = self.conn.describe_db_clusters(DBClusterIdentifier=db_cluster_id)
        except self.conn.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "DBClusterNotFoundFault":
                raise AirflowNotFoundException(e)
            raise e
        return response["DBClusters"][0]["Status"].lower()

    def wait_for_db_cluster_state(
        self, db_cluster_id: str, target_state: str, check_interval: int = 30, max_attempts: int = 40
    ) -> None:
        """
        Polls until the target state is reached.
        An error is raised after a max number of attempts.

        :param db_cluster_id: The ID of the target DB cluster.
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made

        """

        def poke():
            return self.get_db_cluster_state(db_cluster_id)

        target_state = target_state.lower()
        self._wait_for_state(poke, target_state, check_interval, max_attempts)
        self.log.info("DB cluster snapshot '%s' reached the '%s' state", db_cluster_id, target_state)

    def _wait_for_state(
        self,
        poke: Callable[..., str],
        target_state: str,
        check_interval: int,
        max_attempts: int,
    ) -> None:
        """
        Polls the poke function for the current state until it reaches the target_state.

        :param poke: A function that returns the current state of the target resource as a string.
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made
        """
        state = poke()
        tries = 1
        while state != target_state:
            self.log.info("Current state is %s", state)
            if tries >= max_attempts:
                raise AirflowException("Max attempts exceeded")
            time.sleep(check_interval)
            state = poke()
            tries += 1
