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
"""Amazon S3 Vectors operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.sdk import Context


class S3VectorsCreateVectorBucketOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Create an Amazon S3 Vectors vector bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3VectorsCreateVectorBucketOperator`

    :param vector_bucket_name: The name of the vector bucket to create (3-63 chars).
    :param encryption_configuration: Optional encryption config dict with keys
        ``sseType`` (``AES256`` or ``aws:kms``) and optionally ``kmsKeyArn``.
    :param tags: Optional dict of tags to apply to the vector bucket.
    :param if_exists: Behavior when the bucket already exists.
        ``"fail"`` raises an error, ``"skip"`` returns the existing bucket ARN.
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "vector_bucket_name",
    )

    def __init__(
        self,
        *,
        vector_bucket_name: str,
        encryption_configuration: dict[str, Any] | None = None,
        tags: dict[str, str] | None = None,
        if_exists: Literal["fail", "skip"] = "skip",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.vector_bucket_name = vector_bucket_name
        self.encryption_configuration = encryption_configuration
        self.tags = tags
        self.if_exists = if_exists

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "s3vectors"}

    def execute(self, context: Context) -> str:
        kwargs: dict[str, Any] = prune_dict(
            {
                "vectorBucketName": self.vector_bucket_name,
                "encryptionConfiguration": self.encryption_configuration,
                "tags": self.tags,
            }
        )
        try:
            response = self.hook.conn.create_vector_bucket(**kwargs)
            vector_bucket_arn = response["vectorBucketArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException" and self.if_exists == "skip":
                self.log.info("Vector bucket %s already exists, skipping.", self.vector_bucket_name)
                response = self.hook.conn.get_vector_bucket(vectorBucketName=self.vector_bucket_name)
                vector_bucket_arn = response["vectorBucketArn"]
            else:
                raise
        self.log.info("Vector bucket %s: %s", self.vector_bucket_name, vector_bucket_arn)
        return vector_bucket_arn


class S3VectorsDeleteVectorBucketOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Delete an Amazon S3 Vectors vector bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3VectorsDeleteVectorBucketOperator`

    :param vector_bucket_name: The name of the vector bucket to delete.
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "vector_bucket_name",
    )

    def __init__(
        self,
        *,
        vector_bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.vector_bucket_name = vector_bucket_name

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "s3vectors"}

    def execute(self, context: Context) -> None:
        self.hook.conn.delete_vector_bucket(vectorBucketName=self.vector_bucket_name)
        self.log.info("Deleted vector bucket %s", self.vector_bucket_name)


class S3VectorsCreateIndexOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Create an index in an Amazon S3 Vectors vector bucket.

    An index stores vectors and supports similarity search queries.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3VectorsCreateIndexOperator`

    :param vector_bucket_name: The name of the vector bucket. (templated)
    :param index_name: The name of the index to create. (templated)
    :param data_type: The data type for vectors (e.g. ``float32``). (templated)
    :param dimension: The number of dimensions for each vector.
    :param distance_metric: The distance metric for similarity search (e.g. ``cosine``, ``euclidean``).
    :param metadata_configuration: Optional metadata configuration dict.
    :param if_exists: Behavior when the index already exists.
        ``"fail"`` raises an error, ``"skip"`` returns the existing index ARN.
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "vector_bucket_name",
        "index_name",
        "data_type",
        "distance_metric",
    )
    template_fields_renderers = {"metadata_configuration": "json"}

    def __init__(
        self,
        *,
        vector_bucket_name: str,
        index_name: str,
        data_type: str,
        dimension: int,
        distance_metric: str = "cosine",
        metadata_configuration: dict[str, Any] | None = None,
        if_exists: Literal["fail", "skip"] = "skip",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.vector_bucket_name = vector_bucket_name
        self.index_name = index_name
        self.data_type = data_type
        self.dimension = dimension
        self.distance_metric = distance_metric
        self.metadata_configuration = metadata_configuration
        self.if_exists = if_exists

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "s3vectors"}

    def execute(self, context: Context) -> str:
        self.log.info("Creating index %s in vector bucket %s", self.index_name, self.vector_bucket_name)
        kwargs: dict[str, Any] = prune_dict(
            {
                "vectorBucketName": self.vector_bucket_name,
                "indexName": self.index_name,
                "dataType": self.data_type,
                "dimension": self.dimension,
                "distanceMetric": self.distance_metric,
                "metadataConfiguration": self.metadata_configuration,
            }
        )
        try:
            response = self.hook.conn.create_index(**kwargs)
            index_arn = response["indexArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException" and self.if_exists == "skip":
                self.log.info("Index %s already exists, skipping.", self.index_name)
                response = self.hook.conn.get_index(
                    vectorBucketName=self.vector_bucket_name, indexName=self.index_name
                )
                index_arn = response["index"]["indexArn"]
            else:
                raise
        self.log.info("Index %s: %s", self.index_name, index_arn)
        return index_arn


class S3VectorsDeleteIndexOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Delete an index from an Amazon S3 Vectors vector bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3VectorsDeleteIndexOperator`

    :param vector_bucket_name: The name of the vector bucket. (templated)
    :param index_name: The name of the index to delete. (templated)
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = (
        *AwsBaseOperator.template_fields,
        "vector_bucket_name",
        "index_name",
    )

    def __init__(
        self,
        *,
        vector_bucket_name: str,
        index_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.vector_bucket_name = vector_bucket_name
        self.index_name = index_name

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "s3vectors"}

    def execute(self, context: Context) -> None:
        self.log.info("Deleting index %s from vector bucket %s", self.index_name, self.vector_bucket_name)
        self.hook.conn.delete_index(vectorBucketName=self.vector_bucket_name, indexName=self.index_name)
        self.log.info("Deleted index %s", self.index_name)
