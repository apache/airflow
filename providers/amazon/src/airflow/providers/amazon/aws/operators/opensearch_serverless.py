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
"""Amazon OpenSearch Serverless operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.opensearch_serverless import OpenSearchServerlessHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.sdk import Context


class OpenSearchServerlessCreateCollectionOperator(AwsBaseOperator[OpenSearchServerlessHook]):
    """
    Create an Amazon OpenSearch Serverless collection.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenSearchServerlessCreateCollectionOperator`

    :param collection_name: The name of the collection. (templated)
    :param collection_type: The type of collection (SEARCH, TIMESERIES, VECTORSEARCH). (templated)
    :param description: Optional description. (templated)
    :param standby_replicas: Whether to use standby replicas (ENABLED or DISABLED).
    :param tags: Optional list of tag dicts.
    :param if_exists: Behavior when the collection already exists.
        ``"fail"`` raises an error, ``"skip"`` logs and returns.
    """

    aws_hook_class = OpenSearchServerlessHook
    template_fields: Sequence[str] = aws_template_fields("collection_name", "collection_type", "description")

    def __init__(
        self,
        *,
        collection_name: str,
        collection_type: str = "SEARCH",
        description: str | None = None,
        standby_replicas: str | None = None,
        tags: list[dict[str, str]] | None = None,
        if_exists: Literal["fail", "skip"] = "skip",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.collection_type = collection_type
        self.description = description
        self.standby_replicas = standby_replicas
        self.tags = tags
        self.if_exists = if_exists

    def execute(self, context: Context) -> str:
        self.log.info("Creating OpenSearch Serverless collection %s", self.collection_name)
        kwargs: dict[str, Any] = prune_dict(
            {
                "name": self.collection_name,
                "type": self.collection_type,
                "description": self.description,
                "standbyReplicas": self.standby_replicas,
                "tags": self.tags,
            }
        )
        try:
            response = self.hook.conn.create_collection(**kwargs)
            collection_id = response["createCollectionDetail"]["id"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException" and self.if_exists == "skip":
                self.log.info("Collection %s already exists, skipping.", self.collection_name)
                collections = self.hook.conn.batch_get_collection(names=[self.collection_name])
                details = collections.get("collectionDetails", [])
                if not details:
                    raise RuntimeError(
                        f"Collection {self.collection_name} reported as existing but not found"
                    )
                collection_id = details[0]["id"]
            else:
                raise
        self.log.info("Collection %s: %s", self.collection_name, collection_id)
        return collection_id
