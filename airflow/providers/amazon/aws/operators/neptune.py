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
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook
from airflow.providers.amazon.aws.utils.neptune import NeptuneDbType

if TYPE_CHECKING:
    from airflow.utils.context import Context


class NeptuneStartDbOperator(BaseOperator):
    """
    Starts a Neptune DB cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneStartDbOperator`

    :param db_identifier: The AWS identifier of the DB to start
    :param db_type: Type of the DB - either "instance" or "cluster" (default: "cluster")
    :param aws_conn_id: The Airflow connection used for AWS credentials. (default: "aws_default")
    :param wait_for_completion:  If True, waits for DB to start. (default: True)

    Note: In boto3 supports starting db operator only for cluster and not for instance db_type.
        So, default is maintained as Cluster, however it can be extended once instance db_type is available,
        similar to RDS database implementation
    """

    template_fields = ("db_identifier", "db_type")
    STATES_FOR_STARTING = ["available", "starting"]

    def __init__(
        self,
        *,
        db_identifier: str,
        db_type: NeptuneDbType | str = NeptuneDbType.CLUSTER,
        aws_conn_id: str = "aws_default",
        region_name: str = "us-east-1",
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_identifier = db_identifier
        self.hook = NeptuneHook(aws_conn_id=aws_conn_id, region_name=region_name)
        self.db_identifier = db_identifier
        self.db_type = db_type
        self.aws_conn_id = aws_conn_id
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.db_type = NeptuneDbType(self.db_type)
        start_db_response = None
        if (
            self.hook.get_db_cluster_state(self.db_identifier)
            not in NeptuneStartDbOperator.STATES_FOR_STARTING
        ):
            self._start_db()

        if self.wait_for_completion:
            self._wait_until_db_available()
        return json.dumps(start_db_response, default=str)

    def _start_db(self):
        self.log.info("Starting DB %s '%s'", self.db_type.value, self.db_identifier)
        self.hook.conn.start_db_cluster(DBClusterIdentifier=self.db_identifier)

    def _wait_until_db_available(self):
        self.log.info("Waiting for DB %s to reach 'available' state", self.db_type.value)
        self.hook.wait_for_db_cluster_state(self.db_identifier, target_state="available")


class NeptuneStopDbOperator(BaseOperator):
    """
    Stops a Neptune DB cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneStopDbOperator`

    :param db_identifier: The AWS identifier of the DB to start
    :param db_type: Type of the DB - either "instance" or "cluster" (default: "cluster")
    :param aws_conn_id: The Airflow connection used for AWS credentials. (default: "aws_default")
    :param wait_for_completion:  If True, waits for DB to start. (default: True)

    Note: In boto3 supports starting db operator only for cluster and not for instance db_type.
        So, default is maintained as Cluster, however it can be extended once instance db_type is available,
        similar to RDS database implementation
    """

    template_fields = ("db_identifier", "db_type")
    STATES_FOR_STOPPING = ["stopped", "stopping"]

    def __init__(
        self,
        *,
        db_identifier: str,
        db_type: NeptuneDbType | str = NeptuneDbType.INSTANCE,
        aws_conn_id: str = "aws_default",
        region_name: str = "us-east-1",
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hook = NeptuneHook(aws_conn_id=aws_conn_id, region_name=region_name)
        self.db_identifier = db_identifier
        self.db_type = db_type
        self.aws_conn_id = aws_conn_id
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.db_type = NeptuneDbType(self.db_type)
        stop_db_response = None
        if (
            self.hook.get_db_cluster_state(self.db_identifier)
            not in NeptuneStopDbOperator.STATES_FOR_STOPPING
        ):
            stop_db_response = self._stop_db()
        if self.wait_for_completion:
            self._wait_until_db_stopped()
        return json.dumps(stop_db_response, default=str)

    def _stop_db(self):
        self.log.info("Stopping DB %s '%s'", self.db_type.value, self.db_identifier)
        response = self.hook.conn.stop_db_cluster(DBClusterIdentifier=self.db_identifier)
        return response

    def _wait_until_db_stopped(self):
        self.log.info("Waiting for DB %s to reach 'stopped' state", self.db_type.value)
        self.hook.wait_for_db_cluster_state(self.db_identifier, target_state="stopped")


__all__ = ["NeptuneStartDbOperator", "NeptuneStopDbOperator"]
