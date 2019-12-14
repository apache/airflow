# -*- coding: utf-8 -*-
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
This module contains Google Cloud Storage to SFTP operator.
"""
import os
from tempfile import NamedTemporaryFile
from typing import Optional

from airflow import AirflowException
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.providers.sftp.hooks.sftp_hook import SFTPHook
from airflow.utils.decorators import apply_defaults

WILDCARD = "*"


class GoogleCloudStorageToSFTPOperator(BaseOperator):
    """
    Transfer files from a Google Cloud Storage bucket to SFTP server.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCloudStorageToSFTPOperator`

    :param source_bucket: The source Google Cloud Storage bucket where the
         object is. (templated)
    :type source_bucket: str
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket. (templated)
        You can use only one wildcard for objects (filenames) within your
        bucket. The wildcard can appear inside the object name or at the
        end of the object name. Appending a wildcard to the bucket name is
        unsupported.
    :type source_object: str
    :param destination_path: The sftp remote path. This is the specified directory path for
        uploading to the SFTP server.
    :type destination_path: str
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :type move_object: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("source_bucket", "source_object", "destination_path")
    ui_color = "#f0eee4"

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        source_bucket: str,
        source_object: str,
        destination_path: str,
        move_object: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        sftp_conn_id: str = "ssh_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_path = destination_path
        self.move_object = move_object
        self.gcp_conn_id = gcp_conn_id
        self.sftp_conn_id = sftp_conn_id
        self.delegate_to = delegate_to
        self.sftp_dirs = None

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        sftp_hook = SFTPHook(self.sftp_conn_id)

        if WILDCARD in self.source_object:
            total_wildcards = self.source_object.count(WILDCARD)
            if total_wildcards > 1:
                raise AirflowException(
                    "Only one wildcard '*' is allowed in source_object parameter. "
                    "Found {} in {}.".format(total_wildcards, self.source_object)
                )

            prefix, delimiter = self.source_object.split(WILDCARD, 1)
            objects = gcs_hook.list(
                self.source_bucket, prefix=prefix, delimiter=delimiter
            )

            for source_object in objects:
                destination_path = os.path.join(self.destination_path, source_object)
                self._copy_single_object(
                    gcs_hook, sftp_hook, source_object, destination_path
                )

            self.log.info(
                "Done. Uploaded '%d' files to %s", len(objects), self.destination_path
            )
        else:
            destination_path = os.path.join(self.destination_path, self.source_object)
            self._copy_single_object(
                gcs_hook, sftp_hook, self.source_object, destination_path
            )
            self.log.info(
                "Done. Uploaded '%s' file to %s", self.source_object, destination_path
            )

    def _copy_single_object(
        self,
        gcs_hook: GoogleCloudStorageHook,
        sftp_hook: SFTPHook,
        source_object: str,
        destination_path: str,
    ) -> None:
        """
        Helper function to copy single object.
        """
        self.log.info(
            "Executing copy of gs://%s/%s to %s",
            self.source_bucket,
            source_object,
            destination_path,
        )

        dir_path = os.path.dirname(destination_path)
        sftp_hook.create_directory(dir_path)

        with NamedTemporaryFile("w") as tmp:
            gcs_hook.download(
                bucket_name=self.source_bucket,
                object_name=source_object,
                filename=tmp.name,
            )
            sftp_hook.store_file(destination_path, tmp.name)

        if self.move_object:
            self.log.info(
                "Executing delete of gs://%s/%s", self.source_bucket, source_object
            )
            gcs_hook.delete(self.source_bucket, source_object)
