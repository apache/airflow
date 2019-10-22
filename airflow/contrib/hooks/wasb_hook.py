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
#
"""
This module contains integration with Azure Blob Storage.

It communicate via the Window Azure Storage Blob protocol. Make sure that a
Airflow connection of type `wasb` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection `wasb_default` for an example).
"""
from azure.storage.blob import BlockBlobService

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class WasbHook(BaseHook):
    """
    Interacts with Azure Blob Storage through the ``wasb://`` protocol.

    Additional options passed in the 'extra' field of the connection will be
    passed to the `BlockBlockService()` constructor. For example, authenticate
    using a SAS token by adding {"sas_token": "YOUR_TOKEN"}.

    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    """

    def __init__(self, wasb_conn_id='wasb_default'):
        self.conn_id = wasb_conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        """Return the BlockBlobService object."""
        conn = self.get_connection(self.conn_id)
        service_options = conn.extra_dejson
        return BlockBlobService(account_name=conn.login,
                                account_key=conn.password, **service_options)

    def check_for_blob(self, container_name, blob_name, **kwargs):
        """
        Check if a blob exists on Azure Blob Storage.

        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that
            `BlockBlobService.exists()` takes.
        :type kwargs: object
        :return: True if the blob exists, False otherwise.
        :rtype: bool
        """
        return self.connection.exists(container_name, blob_name, **kwargs)

    def check_for_prefix(self, container_name, prefix, **kwargs):
        """
        Check if a prefix exists on Azure Blob storage.

        :param container_name: Name of the container.
        :type container_name: str
        :param prefix: Prefix of the blob.
        :type prefix: str
        :param kwargs: Optional keyword arguments that
            `BlockBlobService.list_blobs()` takes.
        :type kwargs: object
        :return: True if blobs matching the prefix exist, False otherwise.
        :rtype: bool
        """
        matches = self.connection.list_blobs(container_name, prefix,
                                             num_results=1, **kwargs)
        return len(list(matches)) > 0

    def load_file(self, file_path, container_name, blob_name, **kwargs):
        """
        Upload a file to Azure Blob Storage.

        :param file_path: Path to the file to load.
        :type file_path: str
        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that
            `BlockBlobService.create_blob_from_path()` takes.
        :type kwargs: object
        """
        # Reorder the argument order from airflow.hooks.S3_hook.load_file.
        self.connection.create_blob_from_path(container_name, blob_name,
                                              file_path, **kwargs)

    def load_string(self, string_data, container_name, blob_name, **kwargs):
        """
        Upload a string to Azure Blob Storage.

        :param string_data: String to load.
        :type string_data: str
        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that
            `BlockBlobService.create_blob_from_text()` takes.
        :type kwargs: object
        """
        # Reorder the argument order from airflow.hooks.S3_hook.load_string.
        self.connection.create_blob_from_text(container_name, blob_name,
                                              string_data, **kwargs)

    def get_file(self, file_path, container_name, blob_name, **kwargs):
        """
        Download a file from Azure Blob Storage.

        :param file_path: Path to the file to download.
        :type file_path: str
        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that
            `BlockBlobService.create_blob_from_path()` takes.
        :type kwargs: object
        """
        return self.connection.get_blob_to_path(container_name, blob_name,
                                                file_path, **kwargs)

    def read_file(self, container_name, blob_name, **kwargs):
        """
        Read a file from Azure Blob Storage and return as a string.

        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that
            `BlockBlobService.create_blob_from_path()` takes.
        :type kwargs: object
        """
        return self.connection.get_blob_to_text(container_name,
                                                blob_name,
                                                **kwargs).content

    def delete_file(self, container_name, blob_name, is_prefix=False,
                    ignore_if_missing=False, **kwargs):
        """
        Delete a file from Azure Blob Storage.

        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param is_prefix: If blob_name is a prefix, delete all matching files
        :type is_prefix: bool
        :param ignore_if_missing: if True, then return success even if the
            blob does not exist.
        :type ignore_if_missing: bool
        :param kwargs: Optional keyword arguments that
            `BlockBlobService.create_blob_from_path()` takes.
        :type kwargs: object
        """

        if is_prefix:
            blobs_to_delete = [
                blob.name for blob in self.connection.list_blobs(
                    container_name, prefix=blob_name, **kwargs
                )
            ]
        elif self.check_for_blob(container_name, blob_name):
            blobs_to_delete = [blob_name]
        else:
            blobs_to_delete = []

        if not ignore_if_missing and len(blobs_to_delete) == 0:
            raise AirflowException('Blob(s) not found: {}'.format(blob_name))

        for blob_uri in blobs_to_delete:
            self.log.info("Deleting blob: " + blob_uri)
            self.connection.delete_blob(container_name,
                                        blob_uri,
                                        delete_snapshots='include',
                                        **kwargs)
