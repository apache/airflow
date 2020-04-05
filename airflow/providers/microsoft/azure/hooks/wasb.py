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
from typing import Optional, List

import azure.storage.blob as storage_blob
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class WasbHook(BaseHook):
    """
    Interacts with Azure Blob Storage.

    Additional options passed in the 'extra' field of the connection will be
    passed to the `BlockBlockService()` constructor. For example, authenticate
    using a SAS token by adding {"sas_token": "YOUR_TOKEN"}.

    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param public_read: Whether an anonymous public read access should be used.
        default is False
    :type public_read: bool
    """

    def __init__(self, wasb_conn_id='wasb_default', public_read=False):
        super().__init__()
        self.conn_id = wasb_conn_id
        self.public_read = public_read

    def get_conn(self):
        """Return the BlobServiceClient object."""
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson
        def _get_required_param(name):
            """Extract required parameter from extra JSON, raise exception if not found"""
            value = extra.get(name)
            if not value:
                raise AirflowException(
                    'Extra connection option is missing required parameter: `{}`'.
                    format(name))
            return value
        if self.public_read:
            # Here we use anonymous public read
            # more info
            # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-manage-access-to-resources
            return storage_blob.BlobServiceClient(account_url=conn.host)
        if extra.get('connection_string'):
            # connection_string auth takes priority
            return storage_blob.BlobServiceClient.from_connection_string(extra.get('connection_string'))
        if extra.get('shared_access_key'):
            # using shared access key
            return storage_blob.BlobServiceClient(account_url=conn.host,
                                     credential=extra.get('shared_access_key'))
        if extra.get('tenant_id'):
            # use Active Directory auth
            app_id = _get_required_param('application_id')
            app_secret = _get_required_param("application_secret")
            token_credential = ClientSecretCredential(
                extra.get('tenant_id'),
                app_id,
                app_secret
            )
            return storage_blob.BlobServiceClient(account_url=conn.host, credential=token_credential)

    def _get_container_client(self, container_name: str) -> storage_blob.ContainerClient:
        """

        Instantiates a container client

        :param container_name: The name of the container
        :type container_name: str
        :return: ContainerClient
        """
        return self.get_conn().get_container_client(container_name)

    def create_container(self, container_name: str):
        """
        Create container object if not already existing
        :param container_name:
        """
        try:
            self.log.info('Attempting to create a container: %s', container_name)
            self._get_container_client(container_name).create_container()
            self.log.info("Created container: %s", container_name)
        except ResourceExistsError:
            self.log.info("Container %s already exists", container_name)


    def delete_container(self, container_name:str):
        """
        Delete a container object

        :param container_name: The name of the container
        :type container_name: str
        """
        # TODO catch exception
        self._get_container_client(container_name).delete_container()

    def list(self, container_name: str, name_starts_with: Optional[str]=None,
                   include: Optional[List[str]]=None, delimiter: Optional[str]='/', **kwargs):
        """
        List blobs in a given container

        :param container_name: The name of the container
        :type container_name: str

        :param name_starts_with: Filters the results to return only blobs whose names
            begin with the specified prefix.
        :type name_starts_with: str

        :param include: Specifies one or more additional datasets to include in the
            response. Options include: 'snapshots', 'metadata', 'uncommittedblobs',
             'copy', 'deleted'.
        :type include: List[str]
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        :type delimiter: str
        """
        container = self._get_container_client(container_name)
        blob_list = []
        while True:
            blobs = container.walk_blobs(
                name_starts_with=name_starts_with,
                include=include,
                delimiter=delimiter,
                **kwargs
            )
            blob_names = []
            for blob in blobs:
                blob_names.append(blob.name)
            blob_list += blob_names
            next_ = blobs.next()
            if next_ is None:
                # next_ iterator is None
                break
        return  blob_list

    def _get_blobclient(self, container_name: str, blob_name: str) -> storage_blob.BlobClient:
        """
        Instantiates a blob client

        :param container_name: The name of the blob container
        :type container_name: str

        :param blob_name: The name of the blob. This needs not be existing
        :type blob_name: str
        """
        container_client = self._get_container_client(container_name)
        return container_client.get_blob_client(blob_name)

    def upload(self,container_name, blob_name, data, blob_type: str = 'BlockBlob',
                    length: Optional[int] = None, **kwargs):
        """
        Creates a new blob from a data source with automatic chunking.

        :param container_name: The name of the container to upload data
        :type container_name: str

        :param blob_name: The name of the blob to upload. This need not exist in the container
        :type blob_name: str

        :param data: The blob data to upload

        :param blob_type: The type of the blob. This can be either BlockBlob,
            PageBlob or AppendBlob. The default value is BlockBlob.
        :type blob_type: storage.BlobType

        :param length: Number of bytes to read from the stream. This is optional,
            but should be supplied for optimal performance.
        :type length: int
        """
        blob_client = self._get_blobclient(container_name, blob_name)
        return blob_client.upload(data, blob_type, length=length, **kwargs)

    def download(self, container_name, blob_name, dest_file: str=None,
                 offset: Optional[int]=None, length: Optional[int] = None, **kwargs):
        """
        Downloads a blob to the StorageStreamDownloader

        :param container_name: The name of the container containing the blob
        :type container_name: str

        :param blob_name: The name of the blob to download
        :type blob_name: str

        :param dest_file: The name to use to save the download
        :type dest_file: str

        :param offset: Start of byte range to use for downloading a section of the blob.
            Must be set if length is provided.
        :type offset: int

        :param length: Number of bytes to read from the stream.
        :type length: int
        """

        blob_client = self._get_blobclient(container_name, blob_name)
        with open(dest_file, 'wb') as blob:
            download_stream = blob_client.download_blob(offset=offset, length=length, **kwargs)
            blob.write(download_stream.readall())


    def upload_page(self, container_name, blob_name, page, offset, length, **kwargs):
        pass

    def append_block(self, container_name, blob_name, data, length=None, **kwargs):
        pass

    def append_block_from_url(self, container_name, blob_name, data, ):
        pass
