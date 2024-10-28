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

import os
import tempfile
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

try:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
except ModuleNotFoundError as e:
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureBlobStorageToS3Operator(BaseOperator):
    """
    Operator transfers data from Azure Blob Storage to specified bucket in Amazon S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureBlobStorageToGCSOperator`

    :param wasb_conn_id: Reference to the wasb connection.
    :param container_name: Name of the container
    :param prefix: Prefix string which filters objects whose name begin with
        this prefix. (templated)
    :param delimiter: The delimiter by which you want to filter the objects. (templated)
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :param aws_conn_id: Connection id of the S3 connection to use
    :param dest_s3_key: The base S3 key to be used to store the files. (templated)
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param dest_s3_extra_args: Extra arguments that may be passed to the download/upload operations.
    :param replace: Whether or not to verify the existence of the files in the
        destination bucket.
        By default is set to False
        If set to True, will upload all the files replacing the existing ones in
        the destination bucket.
        If set to False, will upload only the files that are in the origin but not
        in the destination bucket.
    :param s3_acl_policy: Optional The string to specify the canned ACL policy for the
        object to be uploaded in S3
    :param wasb_extra_kargs: kwargs to pass to WasbHook
    :param s3_extra_kargs: kwargs to pass to S3Hook
    """

    template_fields: Sequence[str] = (
        "container_name",
        "prefix",
        "delimiter",
        "dest_s3_key",
    )

    def __init__(
        self,
        *,
        wasb_conn_id: str = "wasb_default",
        container_name: str,
        prefix: str | None = None,
        delimiter: str = "",
        aws_conn_id: str | None = "aws_default",
        dest_s3_key: str,
        dest_verify: str | bool | None = None,
        dest_s3_extra_args: dict | None = None,
        replace: bool = False,
        s3_acl_policy: str | None = None,
        wasb_extra_args: dict | None = None,
        s3_extra_args: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.prefix = prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.dest_s3_key = dest_s3_key
        self.dest_verify = dest_verify
        self.dest_s3_extra_args = dest_s3_extra_args or {}
        self.replace = replace
        self.s3_acl_policy = s3_acl_policy
        self.wasb_extra_args = wasb_extra_args or {}
        self.s3_extra_args = s3_extra_args or {}

    def execute(self, context: Context) -> list[str]:
        # list all files in the Azure Blob Storage container
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id, **self.wasb_extra_args)
        s3_hook = S3Hook(
            aws_conn_id=self.aws_conn_id,
            verify=self.dest_verify,
            extra_args=self.dest_s3_extra_args,
            **self.s3_extra_args,
        )

        self.log.info(
            "Getting list of the files in Container: %r; Prefix: %r; Delimiter: %r.",
            self.container_name,
            self.prefix,
            self.delimiter,
        )

        files = wasb_hook.get_blobs_list_recursive(
            container_name=self.container_name,
            prefix=self.prefix,
            endswith=self.delimiter,
        )

        if not self.replace:
            # if we are not replacing -> list all files in the S3 bucket
            # and only keep those files which are present in
            # Azure Blob Storage and not in S3
            bucket_name, prefix = S3Hook.parse_s3_url(self.dest_s3_key)
            # look for the bucket and the prefix to avoid look into
            # parent directories/keys
            existing_files = s3_hook.list_keys(bucket_name, prefix=prefix)
            # in case that no files exists, return an empty array to avoid errors
            existing_files = existing_files or []
            # remove the prefix for the existing files to allow the match
            existing_files = [
                file.replace(f"{prefix}/", "", 1) for file in existing_files
            ]
            files = list(set(files) - set(existing_files))

        if files:
            for file in files:
                with tempfile.NamedTemporaryFile() as temp_file:
                    dest_key = os.path.join(self.dest_s3_key, file)
                    self.log.info("Downloading data from blob: %s", file)
                    wasb_hook.get_file(
                        file_path=temp_file.name,
                        container_name=self.container_name,
                        blob_name=file,
                    )

                    self.log.info("Uploading data to s3: %s", dest_key)
                    s3_hook.load_file(
                        filename=temp_file.name,
                        key=dest_key,
                        replace=self.replace,
                        acl_policy=self.s3_acl_policy,
                    )
            self.log.info("All done, uploaded %d files to S3", len(files))
        else:
            self.log.info("All files are already in sync!")
        return files
