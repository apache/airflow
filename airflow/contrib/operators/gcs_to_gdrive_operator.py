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
This module contains a Google Cloud Storage operator.
"""
import tempfile
from typing import Optional

from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.contrib.hooks.gdrive_hook import GoogleDriveHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

WILDCARD = "*"


class GcsToGDriveOperator(BaseOperator):
    """
    Copies objects from a Google Cloud Storage service service to Google Drive service, with renaming
    if requested.

    Using this operator requires the following OAuth 2.0 scope:

    .. code-block:: none

        https://www.googleapis.com/auth/drive

    :param source_bucket: The source Google Cloud Storage bucket where the object is. (templated)
    :type source_bucket: str
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket. (templated)
        You can use only one wildcard for objects (filenames) within your bucket. The wildcard can appear
        inside the object name or at the end of the object name. Appending a wildcard to the bucket name
        is unsupported.
    :type source_object: str
    :param destination_object: The destination name of the object in the destination Google Drive
        service. (templated)
        If a wildcard is supplied in the source_object argument, this is the prefix that will be prepended
        to the final destination objects' paths.
        Note that the source path's part before the wildcard will be removed;
        if it needs to be retained it should be appended to destination_object.
        For example, with prefix ``foo/*`` and destination_object ``blah/``, the file ``foo/baz`` will be
        copied to ``blah/baz``; to retain the prefix write the destination_object as e.g. ``blah/foo``, in
        which case the copied file will be named ``blah/foo/baz``.
    :type destination_object: str
    :param move_object: When move object is True, the object is moved instead of copied to the new location.
        This is the equivalent of a mv command as opposed to a cp command.
    :type move_object: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("source_bucket", "source_object", "destination_object")
    ui_color = "#f0eee4"

    @apply_defaults
    def __init__(
        self,
        source_bucket: str,
        source_object: str,
        destination_object: str = None,
        move_object: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_object = destination_object
        self.move_object = move_object
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.gcs_hook = None  # type: Optional[GoogleCloudStorageHook]
        self.gdrive_hook = None  # type: Optional[GoogleDriveHook]

    def execute(self, context):

        self.gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )
        self.gdrive_hook = GoogleDriveHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)

        if WILDCARD in self.source_object:
            total_wildcards = self.source_object.count(WILDCARD)
            if total_wildcards > 1:
                error_msg = (
                    "Only one wildcard '*' is allowed in source_object parameter. "
                    "Found {} in {}.".format(total_wildcards, self.source_object)
                )

                raise AirflowException(error_msg)

            prefix, delimiter = self.source_object.split(WILDCARD, 1)
            objects = self.gcs_hook.list(self.source_bucket, prefix=prefix, delimiter=delimiter)

            for source_object in objects:
                if self.destination_object is None:
                    destination_object = source_object
                else:
                    destination_object = source_object.replace(prefix, self.destination_object, 1)

                self._copy_single_object(source_object=source_object, destination_object=destination_object)
        else:
            self._copy_single_object(
                source_object=self.source_object, destination_object=self.destination_object
            )

    def _copy_single_object(self, source_object, destination_object):
        self.log.info(
            "Executing copy of gs://%s/%s to gdrive://%s",
            self.source_bucket,
            source_object,
            destination_object,
        )

        with tempfile.NamedTemporaryFile() as file:
            filename = file.name
            self.gcs_hook.download(
                bucket_name=self.source_bucket, object_name=source_object, filename=filename
            )
            self.gdrive_hook.upload_file(local_location=filename, remote_location=destination_object)

        if self.move_object:
            self.gcs_hook.delete(self.source_bucket, source_object)
