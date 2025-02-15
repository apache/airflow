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

from airflow.providers.amazon.aws.hooks.glacier import GlacierHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlacierCreateJobOperator(AwsBaseOperator[GlacierHook]):
    """
    Initiate an Amazon Glacier inventory-retrieval job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlacierCreateJobOperator`

    :param aws_conn_id: The reference to the AWS connection details
    :param vault_name: the Glacier vault on which job is executed
    """

    aws_hook_class = GlacierHook
    template_fields: Sequence[str] = aws_template_fields("vault_name")

    def __init__(self, *, vault_name: str, **kwargs):
        super().__init__(**kwargs)
        self.vault_name = vault_name

    def execute(self, context: Context):
        return self.hook.retrieve_inventory(vault_name=self.vault_name)


class GlacierUploadArchiveOperator(AwsBaseOperator[GlacierHook]):
    """
    This operator add an archive to an Amazon S3 Glacier vault.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlacierUploadArchiveOperator`

    :param vault_name: The name of the vault
    :param body: A bytes or seekable file-like object. The data to upload.
    :param checksum: The SHA256 tree hash of the data being uploaded.
        This parameter is automatically populated if it is not provided
    :param archive_description: The description of the archive you are uploading
    :param account_id: (Optional) AWS account ID of the account that owns the vault.
        Defaults to the credentials used to sign the request
    :param aws_conn_id: The reference to the AWS connection details
    """

    aws_hook_class = GlacierHook
    template_fields: Sequence[str] = aws_template_fields("vault_name")

    def __init__(
        self,
        *,
        vault_name: str,
        body: object,
        checksum: str | None = None,
        archive_description: str | None = None,
        account_id: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.account_id = account_id
        self.vault_name = vault_name
        self.body = body
        self.checksum = checksum
        self.archive_description = archive_description

    def execute(self, context: Context):
        return self.hook.conn.upload_archive(
            accountId=self.account_id,
            vaultName=self.vault_name,
            archiveDescription=self.archive_description,
            body=self.body,
            checksum=self.checksum,
        )
