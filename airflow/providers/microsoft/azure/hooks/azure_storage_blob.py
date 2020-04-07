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

from typing import List, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class AzureStorageBlobHook(BaseHook):
    """
    Interacts with Azure Blob Storage.

    :param azure_blob_conn_id: Reference to the wasb connection.
    :type azure_blob_conn_id: str
    :param public_read: Whether an anonymous public read access should be used.
        default is False
    :type public_read: bool
    """

    def __init__(self, azure_blob_conn_id='azure_blob_default', public_read=False):
        super().__init__()
        self.conn_id = azure_blob_conn_id
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
            return BlobServiceClient(account_url=conn.host)
        if extra.get('connection_string'):
            # connection_string auth takes priority
            return BlobServiceClient.from_connection_string(extra.get('connection_string'))
        if extra.get('shared_access_key'):
            # using shared access key
            return BlobServiceClient(account_url=conn.host,
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
            return BlobServiceClient(account_url=conn.host, credential=token_credential)
        if extra.get('sas_token'):
            return BlobServiceClient(account_url=extra.get('sas_token'))
        else:
            raise AirflowException('Unknown connection type')

    def _get_container_client(self, container_name: str) -> ContainerClient:
        """

        Instantiates a container client

        :param container_name: The name of the container
        :type container_name: str
        :return: ContainerClient
        """
        return self.get_conn().get_container_client(container_name)

    def _get_blob_client(self, container_name: str, blob_name: str) -> BlobClient:
        """
        Instantiates a blob client

        :param container_name: The name of the blob container
        :type container_name: str

        :param blob_name: The name of the blob. This needs not be existing
        :type blob_name: str
        """
        container_client = self.create_container(container_name)
        return container_client.get_blob_client(blob_name)

    def create_container(self, container_name: str):
        """
        Create container object if not already existing
        :param container_name:
        """
        container_client = self._get_container_client(container_name)
        try:
            self.log.info('Attempting to create container: %s', container_name)
            container_client.create_container()
            self.log.info("Created container: %s", container_name)
            return container_client
        except ResourceExistsError:
            self.log.info("Container %s already exists", container_name)
            return container_client

    def delete_container(self, container_name: str):
        """
        Delete a container object

        :param container_name: The name of the container
        :type container_name: str
        """
        try:
            self.log.info('Attempting to delete container: %s', container_name)
            self._get_container_client(container_name).delete_container()
            self.log.info('Deleted container: %s', container_name)
        except ResourceNotFoundError:
            self.log.info('Container %s not found', container_name)

    def list(self, container_name: str, name_starts_with: Optional[str] = None,
             include: Optional[List[str]] = None, delimiter: Optional[str] = '/', **kwargs):
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
        blobs = container.walk_blobs(
            name_starts_with=name_starts_with,
            include=include,
            delimiter=delimiter,
            **kwargs
        )
        for blob in blobs:
            blob_list.append(blob.name)
        return blob_list

    def upload(self, container_name, blob_name, data, blob_type: str = 'BlockBlob',
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
        blob_client = self._get_blob_client(container_name, blob_name)
        return blob_client.upload_blob(data, blob_type, length=length, **kwargs)

    def download(self, container_name, blob_name, dest_file: str,
                 offset: Optional[int] = None, length: Optional[int] = None, **kwargs):
        """
        Downloads a blob to the StorageStreamDownloader

        :param container_name: The name of the container containing the blob
        :type container_name: str

        :param blob_name: The name of the blob to download
        :type blob_name: str

        :param dest_file: The name to use to save the downloaded file
        :type dest_file: str

        :param offset: Start of byte range to use for downloading a section of the blob.
            Must be set if length is provided.
        :type offset: int

        :param length: Number of bytes to read from the stream.
        :type length: int
        """

        blob_client = self._get_blob_client(container_name, blob_name)
        with open(dest_file, 'wb') as blob:
            self.log.info('Downloading blob: %s', blob_name)
            download_stream = blob_client.download_blob(offset=offset, length=length, **kwargs)
            blob.write(download_stream.readall())

    def copy(self, source_blob, destination_blob,):
        """
        :param source_blob:
        :param destination_blob:
        :return:
        """
        # TODO Add copy from url

    def create_snapshot(self, container_name, blob_name, metadata=None, **kwargs):
        """
        Create a snapshop of the blob object

        :param container_name: The name of the container containing the blob
        :type container_name: str

        :param blob_name: The name of the blob
        :type blob_name: str

        :param metadata: Name-value pairs associated with the blob as metadata.
        :type metadata: Optional[Dict(str,str)]
        """
        snapshot = self._get_blob_client(container_name, blob_name).\
            create_snapshot(metadata=metadata, **kwargs)
        self.log.info("Created a snapshot: %s", snapshot.get('snapshot'))
        return snapshot

    def get_blob_info(self, container_name, blob_name, **kwargs):
        """
        Get blob properties

        :param container_name: The name of the container containing the blob
        :type container_name: str

        :param blob_name: The name of the blob
        :type blob_name: str
        """
        return self._get_blob_client(container_name, blob_name).get_blob_properties(**kwargs)

    def delete_blob(self, container_name: str, blob_name: str,
                    delete_snapshots: Optional[bool] = False, **kwargs):
        """
        Marks the specified blob for deletion. The blob is deleted completely during garbage
        collection

        :param container_name: The name of the container containing the blob
        :type container_name: str

        :param blob_name: The name of the blob
        :type blob_name: str

        :param delete_snapshots: Required if the blob has associated snapshots. Values include:
            "only": Deletes only the blobs snapshots.
            "include": Deletes the blob along with all snapshots.
        :type delete_snapshots: str
        """
        self._get_blob_client(container_name, blob_name).delete_blob(delete_snapshots=delete_snapshots,
                                                                     **kwargs)

    def delete_blobs(self, container_name: str, *blobs, **kwargs):
        """
        Marks the specified blobs or snapshots for deletion.

        :param container_name: The name of the container containing the blobs
        :type container_name: str

        :param blobs: The blobs to delete. This can be a single blob, or multiple values
            can be supplied, where each value is either the name of the blob (str) or BlobProperties.
        :type blobs: Union[str, BlobProperties]
        """
        self._get_container_client(container_name).delete_blobs(*blobs, **kwargs)
        self.log.info("Deleted blobs: %s", blobs)

    def create_page_blob(self, container_name: str, blob_name: str, size: int, **kwargs):
        """"
        Creates a new Page Blob of the specified size.

        :param container_name: The name of the container
        :type container_name: str

        :param blob_name: The name of the page blob
        :type blob_name: The size of the page blob

        :param size: This specifies the maximum size for the page blob
        :type size: int

        """
        return self._get_blob_client(container_name, blob_name).create_page_blob(size, **kwargs)

    def create_append_blob(self, container_name: str, blob_name: str, **kwargs):
        """
        Creates a new Append Blob.

        :param container_name: The name of the container
        :type container_name: str

        :param blob_name: The name of the page blob
        :type blob_name: The size of the page blob
        """
        return self._get_blob_client(container_name, blob_name).create_append_blob(**kwargs)

    def upload_page(self, container_name: str, blob_name: str,
                    page: bytes, offset: int, length: int, **kwargs):
        """
        writes a range of pages to a page blob.

        :param container_name: The name of the container
        :type container_name: str
        :param blob_name: The name of the page blob
        :type blob_name: The size of the page blob
        :param page: Content of the page
        :type page: bytes
        :param offset: Start of byte range to use for writing to a section of the blob.
        :type offset: int
        :param length: Number of bytes to use for writing to a section of the blob.
        :type length: int
        """

        page = self._get_blob_client(container_name, blob_name).\
            upload_page(page, offset, length, **kwargs)
        return page

    def upload_pages_from_url(self, container_name: str, blob_name: str, source_url: str,
                              offset: int, length: int, source_offset: int, **kwargs):
        """
        writes a range of pages to a page blob where the contents are read from a URL

        :param container_name: The name of the container
        :type container_name: str
        :param blob_name: The name of the page blob
        :type blob_name: The size of the page blob
        :param source_url: The URL of the source data. It can point to any Azure
            Blob or File, that is either public or has a shared access signature attached.
        :type source_url: str
        :param page: Content of the page
        :type page: bytes
        :param offset: Start of byte range to use for writing to a section of the blob.
        :type offset: int
        :param length: Number of bytes to use for writing to a section of the blob.
        :type length: int
        :param source_offset: This indicates the start of the range of bytes(inclusive)
            that has to be taken from the copy source
        :type source_offset: int
        """
        return self._get_blob_client(container_name, blob_name).upload_pages_from_url(
            source_url=source_url,
            offset=offset,
            length=length,
            source_offset=source_offset
        )
