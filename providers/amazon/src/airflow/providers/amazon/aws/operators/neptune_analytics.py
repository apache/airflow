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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.neptune import NeptuneAnalyticsHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class CreateNeptuneGraphOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Creates an empty Amazon Neptune Graph database.

    Neptune Analytics is a memory-optimized graph database engine for analytics. With Neptune Analytics, you can get insights and find trends by processing large amounts of graph data in seconds.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneStartDbClusterOperator`

    :param db_cluster_id: The DB cluster identifier of the Neptune DB cluster to be started.
    :param wait_for_completion: Whether to wait for the cluster to start. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the cluster to start.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param waiter_delay: Time in seconds to wait between status checks.
    :param waiter_max_attempts: Maximum number of attempts to check for job completion.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.

    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :return: dictionary with Neptune cluster id
    """

    aws_hook_class = NeptuneAnalyticsHook
    template_fields: Sequence[str] = aws_template_fields()

    def __init__(
        self,
        graph_name: str,
        vector_search_config: dict,
        replica_count: int,
        provisioned_memory: int,
        deletion_protection: bool = False,
        kms_key_id: str | None = None,
        tags: dict | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.graph_name = graph_name
        self.vector_search_config = vector_search_config
        self.replica_count = replica_count
        self.provisioned_memory = provisioned_memory
        self.deletion_protect = deletion_protection
        self.kms_key = kms_key_id
        self.tags = tags
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict:
        self.log.info("Creating graph %s", self.graph_name)
        response = self.hook.create_graph(
            graphName=self.graph_name,
            vectorSearchConfiguration=self.vector_search_config,
            replicaCount=self.replica_count,
            provisionedMemory=self.provisioned_memory,
            deletionProtection=self.deletion_protect,
            kmsKeyIdentifier=self.kms_key,
            tags=self.tags,
        )

        self.log.info("Graph %s in status %s", self.graph_name, response.get("status", "Unknown"))

        if self.deferrable:
            pass
            # TODO add waiter
        if self.wait_for_completion:
            # TODO wait for good status
            pass

        # TODO return - maybe store ID, ARN, Name
