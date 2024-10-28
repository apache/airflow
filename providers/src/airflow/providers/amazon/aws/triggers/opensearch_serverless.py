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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.opensearch_serverless import (
    OpenSearchServerlessHook,
)
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class OpenSearchServerlessCollectionActiveTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an Amazon OpenSearch Serverless Collection reaches the ACTIVE state.

    :param collection_id: A collection ID. You can't provide a name and an ID in the same request.
    :param collection_name: A collection name. You can't provide a name and an ID in the same request.

    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 20)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        collection_id: str | None = None,
        collection_name: str | None = None,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        aws_conn_id: str | None = None,
    ) -> None:
        if not exactly_one(collection_id is None, collection_name is None):
            raise AttributeError(
                "Either collection_ids or collection_names must be provided, not both."
            )

        super().__init__(
            serialized_fields={
                "collection_id": collection_id,
                "collection_name": collection_name,
            },
            waiter_name="collection_available",
            waiter_args={"ids": [collection_id]}
            if collection_id
            else {"names": [collection_name]},
            failure_message="OpenSearch Serverless Collection creation failed.",
            status_message="Status of OpenSearch Serverless Collection is",
            status_queries=["status"],
            return_key="collection_id" if collection_id else "collection_name",
            return_value=collection_id if collection_id else collection_name,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return OpenSearchServerlessHook(aws_conn_id=self.aws_conn_id)
