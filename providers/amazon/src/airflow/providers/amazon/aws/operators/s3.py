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
"""This module contains AWS S3 operators."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

import pytz
from dateutil import parser

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.utils.context import Context

BUCKET_DOES_NOT_EXIST_MSG = "Bucket with name: %s doesn't exist"


class S3CreateBucketOperator(AwsBaseOperator[S3Hook]):
    """
    This operator creates an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3CreateBucketOperator`

    :param bucket_name: This is bucket name you want to create
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("bucket_name")
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name

    def execute(self, context: Context):
        if not self.hook.check_for_bucket(self.bucket_name):
            self.hook.create_bucket(bucket_name=self.bucket_name, region_name=self.region_name)
            self.log.info("Created bucket with name: %s", self.bucket_name)
        else:
            self.log.info("Bucket with name: %s already exists", self.bucket_name)


class S3DeleteBucketOperator(AwsBaseOperator[S3Hook]):
    """
    This operator deletes an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3DeleteBucketOperator`

    :param bucket_name: This is bucket name you want to delete
    :param force_delete: Forcibly delete all objects in the bucket before deleting the bucket
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("bucket_name")
    aws_hook_class = S3Hook

    def __init__(
        self,
        bucket_name: str,
        force_delete: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.force_delete = force_delete

    def execute(self, context: Context):
        if self.hook.check_for_bucket(self.bucket_name):
            self.hook.delete_bucket(bucket_name=self.bucket_name, force_delete=self.force_delete)
            self.log.info("Deleted bucket with name: %s", self.bucket_name)
        else:
            self.log.info("Bucket with name: %s doesn't exist", self.bucket_name)


class S3GetBucketTaggingOperator(AwsBaseOperator[S3Hook]):
    """
    This operator gets tagging from an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3GetBucketTaggingOperator`

    :param bucket_name: This is bucket name you want to reference
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("bucket_name")
    aws_hook_class = S3Hook

    def __init__(self, bucket_name: str, aws_conn_id: str | None = "aws_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name

    def execute(self, context: Context):
        if self.hook.check_for_bucket(self.bucket_name):
            self.log.info("Getting tags for bucket %s", self.bucket_name)
            return self.hook.get_bucket_tagging(self.bucket_name)
        self.log.warning(BUCKET_DOES_NOT_EXIST_MSG, self.bucket_name)
        return None


class S3PutBucketTaggingOperator(AwsBaseOperator[S3Hook]):
    """
    This operator puts tagging for an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3PutBucketTaggingOperator`

    :param bucket_name: The name of the bucket to add tags to.
    :param key: The key portion of the key/value pair for a tag to be added.
        If a key is provided, a value must be provided as well.
    :param value: The value portion of the key/value pair for a tag to be added.
        If a value is provided, a key must be provided as well.
    :param tag_set: A dictionary containing the tags, or a List of key/value pairs.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("bucket_name")
    template_fields_renderers = {"tag_set": "json"}
    aws_hook_class = S3Hook

    def __init__(
        self,
        bucket_name: str,
        key: str | None = None,
        value: str | None = None,
        tag_set: dict | list[dict[str, str]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.value = value
        self.tag_set = tag_set
        self.bucket_name = bucket_name

    def execute(self, context: Context):
        if self.hook.check_for_bucket(self.bucket_name):
            self.log.info("Putting tags for bucket %s", self.bucket_name)
            return self.hook.put_bucket_tagging(
                key=self.key, value=self.value, tag_set=self.tag_set, bucket_name=self.bucket_name
            )
        self.log.warning(BUCKET_DOES_NOT_EXIST_MSG, self.bucket_name)
        return None


class S3DeleteBucketTaggingOperator(AwsBaseOperator[S3Hook]):
    """
    This operator deletes tagging from an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3DeleteBucketTaggingOperator`

    :param bucket_name: This is the name of the bucket to delete tags from.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields("bucket_name")
    aws_hook_class = S3Hook

    def __init__(
        self,
        bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name

    def execute(self, context: Context):
        if self.hook.check_for_bucket(self.bucket_name):
            self.log.info("Deleting tags for bucket %s", self.bucket_name)
            return self.hook.delete_bucket_tagging(self.bucket_name)
        self.log.warning(BUCKET_DOES_NOT_EXIST_MSG, self.bucket_name)
        return None


class S3CopyObjectOperator(AwsBaseOperator[S3Hook]):
    """
    Creates a copy of an object that is already stored in S3.

    Note: the S3 connection used here needs to have access to both
    source and destination bucket/key.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3CopyObjectOperator`

    :param source_bucket_key: The key of the source object. (templated)

        It can be either full s3:// style url or relative path from root level.

        When it's specified as a full s3:// url, please omit source_bucket_name.
    :param dest_bucket_key: The key of the object to copy to. (templated)

        The convention to specify `dest_bucket_key` is the same as `source_bucket_key`.
    :param source_bucket_name: Name of the S3 bucket where the source object is in. (templated)

        It should be omitted when `source_bucket_key` is provided as a full s3:// url.
    :param dest_bucket_name: Name of the S3 bucket to where the object is copied. (templated)

        It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
    :param source_version_id: Version ID of the source object (OPTIONAL)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    :param meta_data_directive: Whether to `COPY` the metadata from the source object or `REPLACE` it with
        metadata that's provided in the request.
    """

    template_fields: Sequence[str] = aws_template_fields(
        "source_bucket_key",
        "dest_bucket_key",
        "source_bucket_name",
        "dest_bucket_name",
    )
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        source_bucket_key: str,
        dest_bucket_key: str,
        source_bucket_name: str | None = None,
        dest_bucket_name: str | None = None,
        source_version_id: str | None = None,
        acl_policy: str | None = None,
        meta_data_directive: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.source_bucket_key = source_bucket_key
        self.dest_bucket_key = dest_bucket_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.source_version_id = source_version_id
        self.acl_policy = acl_policy
        self.meta_data_directive = meta_data_directive

    def execute(self, context: Context):
        self.hook.copy_object(
            self.source_bucket_key,
            self.dest_bucket_key,
            self.source_bucket_name,
            self.dest_bucket_name,
            self.source_version_id,
            self.acl_policy,
            self.meta_data_directive,
        )

    def get_openlineage_facets_on_start(self):
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        dest_bucket_name, dest_bucket_key = self.hook.get_s3_bucket_key(
            self.dest_bucket_name, self.dest_bucket_key, "dest_bucket_name", "dest_bucket_key"
        )

        source_bucket_name, source_bucket_key = self.hook.get_s3_bucket_key(
            self.source_bucket_name, self.source_bucket_key, "source_bucket_name", "source_bucket_key"
        )

        input_dataset = Dataset(
            namespace=f"s3://{source_bucket_name}",
            name=source_bucket_key,
        )
        output_dataset = Dataset(
            namespace=f"s3://{dest_bucket_name}",
            name=dest_bucket_key,
        )

        return OperatorLineage(
            inputs=[input_dataset],
            outputs=[output_dataset],
        )


class S3CreateObjectOperator(AwsBaseOperator[S3Hook]):
    """
    Creates a new object from `data` as string or bytes.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3CreateObjectOperator`

    :param s3_bucket: Name of the S3 bucket where to save the object. (templated)
        It should be omitted when ``s3_key`` is provided as a full s3:// url.
    :param s3_key: The key of the object to be created. (templated)
        It can be either full s3:// style url or relative path from root level.
        When it's specified as a full s3:// url, please omit ``s3_bucket``.
    :param data: string or bytes to save as content.
    :param replace: If True, it will overwrite the key if it already exists
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    :param encoding: The string to byte encoding.
        It should be specified only when `data` is provided as string.
    :param compression: Type of compression to use, currently only gzip is supported.
        It can be specified only when `data` is provided as string.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

    """

    template_fields: Sequence[str] = aws_template_fields("s3_bucket", "s3_key", "data")
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        s3_bucket: str | None = None,
        s3_key: str,
        data: str | bytes,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
        encoding: str | None = None,
        compression: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.data = data
        self.replace = replace
        self.encrypt = encrypt
        self.acl_policy = acl_policy
        self.encoding = encoding
        self.compression = compression

    def execute(self, context: Context):
        s3_bucket, s3_key = self.hook.get_s3_bucket_key(
            self.s3_bucket, self.s3_key, "dest_bucket", "dest_key"
        )

        if isinstance(self.data, str):
            self.hook.load_string(
                self.data,
                s3_key,
                s3_bucket,
                self.replace,
                self.encrypt,
                self.encoding,
                self.acl_policy,
                self.compression,
            )
        else:
            self.hook.load_bytes(self.data, s3_key, s3_bucket, self.replace, self.encrypt, self.acl_policy)

    def get_openlineage_facets_on_start(self):
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        bucket, key = self.hook.get_s3_bucket_key(self.s3_bucket, self.s3_key, "dest_bucket", "dest_key")

        output_dataset = Dataset(
            namespace=f"s3://{bucket}",
            name=key,
        )

        return OperatorLineage(
            outputs=[output_dataset],
        )


class S3DeleteObjectsOperator(AwsBaseOperator[S3Hook]):
    """
    To enable users to delete single object or multiple objects from a bucket using a single HTTP request.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3DeleteObjectsOperator`

    :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
    :param keys: The key(s) to delete from S3 bucket. (templated)

        When ``keys`` is a string, it's supposed to be the key name of
        the single object to delete.

        When ``keys`` is a list, it's supposed to be the list of the
        keys to delete.

    :param prefix: Prefix of objects to delete. (templated)
        All objects matching this prefix in the bucket will be deleted.
    :param from_datetime: Greater LastModified Date of objects to delete. (templated)
        All objects which LastModified Date is greater than this datetime in the bucket will be deleted.
    :param to_datetime: less LastModified Date of objects to delete. (templated)
        All objects which LastModified Date is less than this datetime in the bucket will be deleted.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields(
        "keys", "bucket", "prefix", "from_datetime", "to_datetime"
    )
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        bucket: str,
        keys: str | list | None = None,
        prefix: str | None = None,
        from_datetime: datetime | str | None = None,
        to_datetime: datetime | str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.keys = keys
        self.prefix = prefix
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

        self._keys: str | list[str] = ""

        if not exactly_one(keys is None, all(var is None for var in [prefix, from_datetime, to_datetime])):
            raise AirflowException(
                "Either keys or at least one of prefix, from_datetime, to_datetime should be set."
            )

    def execute(self, context: Context):
        if not exactly_one(
            self.keys is None, all(var is None for var in [self.prefix, self.from_datetime, self.to_datetime])
        ):
            raise AirflowException(
                "Either keys or at least one of prefix, from_datetime, to_datetime should be set."
            )

        if isinstance(self.keys, (list, str)) and not self.keys:
            return
        # handle case where dates are strings, specifically when sent as template fields and macros.
        if isinstance(self.to_datetime, str):
            self.to_datetime = parser.parse(self.to_datetime).replace(tzinfo=pytz.UTC)

        if isinstance(self.from_datetime, str):
            self.from_datetime = parser.parse(self.from_datetime).replace(tzinfo=pytz.UTC)

        keys = self.keys or self.hook.list_keys(
            bucket_name=self.bucket,
            prefix=self.prefix,
            from_datetime=self.from_datetime,
            to_datetime=self.to_datetime,
        )
        if keys:
            self.hook.delete_objects(bucket=self.bucket, keys=keys)
            self._keys = keys

    def get_openlineage_facets_on_complete(self, task_instance):
        """Implement _on_complete because object keys are resolved in execute()."""
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            LifecycleStateChange,
            LifecycleStateChangeDatasetFacet,
            PreviousIdentifier,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        if not self._keys:
            return OperatorLineage()

        keys = self._keys
        if isinstance(keys, str):
            keys = [keys]

        bucket_url = f"s3://{self.bucket}"
        input_datasets = [
            Dataset(
                namespace=bucket_url,
                name=key,
                facets={
                    "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=LifecycleStateChange.DROP.value,
                        previousIdentifier=PreviousIdentifier(
                            namespace=bucket_url,
                            name=key,
                        ),
                    )
                },
            )
            for key in keys
        ]

        return OperatorLineage(
            inputs=input_datasets,
        )


class S3FileTransformOperator(AwsBaseOperator[S3Hook]):
    """
    Copies data from a source S3 location to a temporary location on the local filesystem.

    Runs a transformation on this file as specified by the transformation
    script and uploads the output to a destination S3 location.

    The locations of the source and the destination files in the local
    filesystem is provided as a first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file. The operator then takes over control and uploads the
    local destination file to S3.

    S3 Select is also available to filter the source contents. Users can
    omit the transformation script if S3 Select expression is specified.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3FileTransformOperator`

    :param source_s3_key: The key to be retrieved from S3. (templated)
    :param dest_s3_key: The key to be written from S3. (templated)
    :param transform_script: location of the executable transformation script
    :param select_expression: S3 Select expression
    :param select_expr_serialization_config: A dictionary that contains input and output serialization configurations for S3 Select.
    :param script_args: arguments for transformation script (templated)
    :param source_aws_conn_id: source s3 connection
    :param source_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
             (unless use_ssl is False), but SSL certificates will not be
             verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
             You can specify this argument if you want to use a different
             CA cert bundle than the one used by botocore.

        This is also applicable to ``dest_verify``.
    :param dest_aws_conn_id: destination s3 connection
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        See: ``source_verify``
    :param replace: Replace dest S3 key if it already exists
    """

    template_fields: Sequence[str] = aws_template_fields("source_s3_key", "dest_s3_key", "script_args")
    template_ext: Sequence[str] = ()
    ui_color = "#f9c915"
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        source_s3_key: str,
        dest_s3_key: str,
        transform_script: str | None = None,
        select_expression=None,
        select_expr_serialization_config: dict[str, dict[str, dict]] | None = None,
        script_args: Sequence[str] | None = None,
        source_aws_conn_id: str | None = "aws_default",
        source_verify: bool | str | None = None,
        dest_aws_conn_id: str | None = "aws_default",
        dest_verify: bool | str | None = None,
        replace: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_s3_key = source_s3_key
        self.source_aws_conn_id = source_aws_conn_id
        self.source_verify = source_verify
        self.dest_s3_key = dest_s3_key
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.replace = replace
        self.transform_script = transform_script
        self.select_expression = select_expression
        self.select_expr_serialization_config = select_expr_serialization_config or {}
        self.script_args = script_args or []
        self.output_encoding = sys.getdefaultencoding()

    def execute(self, context: Context):
        if self.transform_script is None and self.select_expression is None:
            raise AirflowException("Either transform_script or select_expression must be specified")

        # Keep these hooks constructed here since we are using two unique conn_ids
        source_s3 = S3Hook(aws_conn_id=self.source_aws_conn_id, verify=self.source_verify)
        dest_s3 = S3Hook(aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify)

        self.log.info("Downloading source S3 file %s", self.source_s3_key)
        if not source_s3.check_for_key(self.source_s3_key):
            raise AirflowException(f"The source key {self.source_s3_key} does not exist")
        source_s3_key_object = source_s3.get_key(self.source_s3_key)

        with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
            self.log.info("Dumping S3 file %s contents to local file %s", self.source_s3_key, f_source.name)

            if self.select_expression is not None:
                input_serialization = self.select_expr_serialization_config.get("input_serialization")
                output_serialization = self.select_expr_serialization_config.get("output_serialization")
                content = source_s3.select_key(
                    key=self.source_s3_key,
                    expression=self.select_expression,
                    input_serialization=input_serialization,
                    output_serialization=output_serialization,
                )
                f_source.write(content.encode("utf-8"))
            else:
                source_s3_key_object.download_fileobj(Fileobj=f_source)
            f_source.flush()

            if self.transform_script is not None:
                with subprocess.Popen(
                    [self.transform_script, f_source.name, f_dest.name, *self.script_args],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                ) as process:
                    self.log.info("Output:")
                    if process.stdout is not None:
                        for line in iter(process.stdout.readline, b""):
                            self.log.info(line.decode(self.output_encoding).rstrip())

                    process.wait()

                    if process.returncode:
                        raise AirflowException(f"Transform script failed: {process.returncode}")
                    self.log.info(
                        "Transform script successful. Output temporarily located at %s", f_dest.name
                    )

            self.log.info("Uploading transformed file to S3")
            f_dest.flush()
            dest_s3.load_file(
                filename=f_dest.name if self.transform_script else f_source.name,
                key=self.dest_s3_key,
                replace=self.replace,
            )
            self.log.info("Upload successful")

    def get_openlineage_facets_on_start(self):
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        dest_bucket_name, dest_bucket_key = S3Hook.get_s3_bucket_key(
            bucket=None,
            key=self.dest_s3_key,
            bucket_param_name="dest_bucket_name",
            key_param_name="dest_bucket_key",
        )

        source_bucket_name, source_bucket_key = S3Hook.get_s3_bucket_key(
            bucket=None,
            key=self.source_s3_key,
            bucket_param_name="source_bucket_name",
            key_param_name="source_bucket_key",
        )

        input_dataset = Dataset(
            namespace=f"s3://{source_bucket_name}",
            name=source_bucket_key,
        )
        output_dataset = Dataset(
            namespace=f"s3://{dest_bucket_name}",
            name=dest_bucket_key,
        )

        return OperatorLineage(
            inputs=[input_dataset],
            outputs=[output_dataset],
        )


class S3ListOperator(AwsBaseOperator[S3Hook]):
    """
    List all objects from the bucket with the given string prefix in name.

    This operator returns a python list with the name of objects which can be
    used by `xcom` in the downstream task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ListOperator`

    :param bucket: The S3 bucket where to find the objects. (templated)
    :param prefix: Prefix string to filters the objects whose name begin with
        such prefix. (templated)
    :param delimiter: the delimiter marks key hierarchy. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :param apply_wildcard: whether to treat '*' as a wildcard or a plain symbol in the prefix.


    **Example**:
        The following operator would list all the files
        (excluding subfolders) from the S3
        ``customers/2018/04/`` key in the ``data`` bucket. ::

            s3_file = S3ListOperator(
                task_id="list_3s_files",
                bucket="data",
                prefix="customers/2018/04/",
                delimiter="/",
                aws_conn_id="aws_customers_conn",
            )
    """

    template_fields: Sequence[str] = aws_template_fields("bucket", "prefix", "delimiter")
    ui_color = "#ffd700"
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        apply_wildcard: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.apply_wildcard = apply_wildcard

    def execute(self, context: Context):
        self.log.info(
            "Getting the list of files from bucket: %s in prefix: %s (Delimiter %s)",
            self.bucket,
            self.prefix,
            self.delimiter,
        )

        return self.hook.list_keys(
            bucket_name=self.bucket,
            prefix=self.prefix,
            delimiter=self.delimiter,
            apply_wildcard=self.apply_wildcard,
        )


class S3ListPrefixesOperator(AwsBaseOperator[S3Hook]):
    """
    List all subfolders from the bucket with the given string prefix in name.

    This operator returns a python list with the name of all subfolders which
    can be used by `xcom` in the downstream task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ListPrefixesOperator`

    :param bucket: The S3 bucket where to find the subfolders. (templated)
    :param prefix: Prefix string to filter the subfolders whose name begin with
        such prefix. (templated)
    :param delimiter: the delimiter marks subfolder hierarchy. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html


    **Example**:
        The following operator would list all the subfolders
        from the S3 ``customers/2018/04/`` prefix in the ``data`` bucket. ::

            s3_file = S3ListPrefixesOperator(
                task_id="list_s3_prefixes",
                bucket="data",
                prefix="customers/2018/04/",
                delimiter="/",
                aws_conn_id="aws_customers_conn",
            )
    """

    template_fields: Sequence[str] = aws_template_fields("bucket", "prefix", "delimiter")
    ui_color = "#ffd700"
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        delimiter: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter

    def execute(self, context: Context):
        self.log.info(
            "Getting the list of subfolders from bucket: %s in prefix: %s (Delimiter %s)",
            self.bucket,
            self.prefix,
            self.delimiter,
        )

        return self.hook.list_prefixes(bucket_name=self.bucket, prefix=self.prefix, delimiter=self.delimiter)
