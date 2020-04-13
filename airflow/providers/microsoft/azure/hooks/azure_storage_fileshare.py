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
from typing import Optional

from azure.identity import ClientSecretCredential
from azure.storage.fileshare import ShareServiceClient

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class AzureStorageFileShareHook(BaseHook):
    """
    Interacts with Azure Storage Fileshare.

    :param azure_fileshare_conn_id: Reference to the azure storage file share connection.
    :type azure_fileshare_conn_id: str
    """

    def __init__(self, azure_fileshare_conn_id='azure_fileshare_default'):
        super().__init__()
        self.conn_id = azure_fileshare_conn_id

    def get_conn(self):
        """Return the ShareServiceClient object."""
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
        if extra.get('connection_string'):
            # connection_string auth takes priority
            return ShareServiceClient.from_connection_string(extra.get('connection_string'))
        if extra.get('shared_access_key'):
            # using shared access key
            return ShareServiceClient(account_url=conn.host,
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
            return ShareServiceClient(account_url=conn.host, credential=token_credential)
        else:
            raise AirflowException('Unknown connection type')

    def _get_share_client(self, share_name, snapshot: Optional[str] = None, **kwargs):
        """
        A client to interact with a specific share, although that share may not yet exist.

        :param share_name: The name of the share with which to interact.
        :type share_name: Union[str, ShareProperties]
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        """
        share = self.get_conn().get_share_client(share_name=share_name,
                                                 snapshot=snapshot, **kwargs)
        return share

    def _get_directory_client(self, share_name: str, snapshot: Optional[str] = None,
                              directory_path=None):
        """
        Get a client to interact with the specified directory. The directory need not already exist.

        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param directory_path: Path to the specified directory.
        :type directory_path: str
        """
        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.get_directory_client(directory_path=directory_path)

    def _get_file_client(self, share_name: str, snapshot: Optional[str] = None,
                         file_path=None):
        """
        Get a client to interact with the specified file. The file need not already exist.

        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param file_path: Path to the specified file.
        :type file_path: str
        """
        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.get_file_client(file_path=file_path)

    def upload_file(self, source_file: str, share_name: str, snapshot: Optional[str] = None,
                    file_path: Optional[str] = None, **kwargs):
        """
        Upload a file

        :param source_file: The file to upload
        :type source_file: str
        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param file_path: Path to the specified file to upload to.
        :type file_path: str
        """
        file_client = self._get_file_client(share_name, snapshot, file_path=file_path)
        with open(source_file, 'rb') as data:
            file_client.upload_file(data)

    def download_file(self, dest_file: str, share_name: str, snapshot: Optional[str] = None,
                      file_path: Optional[str] = None, offset=None,
                      length=None, **kwargs):
        """
        Download a file to a specified destination

        :param dest_file: The destination to download the file to.
        :type dest_file: str
        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param file_path: Path to the specified file.
        :type file_path: str
        :param offset: Start of byte range to use for downloading a section
            of the file. Must be set if length is provided.
        :type offset: int
        :param length: Number of bytes to read from the stream.
        :type length: int
        """
        if length and not offset:
            raise AirflowException('The offset is required if length is set')

        file_client = self._get_file_client(share_name, snapshot, file_path=file_path)

        with open(dest_file, 'wb') as data:
            stream = file_client.download_file(offset=offset, length=length, **kwargs)
            data.write(stream.readall())

    def create_file(self, size: int, share_name: str, snapshot: Optional[str] = None,
                    file_path: Optional[str] = None, **kwargs):
        """
        Creates a new file

        :param size: Specifies the maximum size for the file, up to 1 TB.
        :type size: int
        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param file_path: Path to the specified file.
        :type file_path: str
        """
        file_client = self._get_file_client(share_name, snapshot, file_path=file_path)

        return file_client.create_file(size=size, **kwargs)

    def delete_file(self, share_name: str, snapshot: Optional[str] = None,
                    file_path: Optional[str] = None, **kwargs):
        """
        Mark specified file for deletion

        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param file_path: Path to the specified file.
        :type file_path: str
        """
        file_client = self._get_file_client(share_name, snapshot, file_path=file_path)
        file_client.delete_file(**kwargs)

    def start_copy_from_url(self, source_url, share_name: str, snapshot: Optional[str] = None,
                            file_path: Optional[str] = None, **kwargs):
        """
        Initiates the copying of data from a source URL into the file referenced by the client.

        :param source_url: Specifies the URL of the source file.
        :type source_url : str
        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param file_path: Path to the specified file.
        :type file_path: str
        """
        file_client = self._get_file_client(share_name, snapshot, file_path=file_path)
        return file_client.start_copy_from_url(source_url, **kwargs)

    def create_share(self, share_name: str, snapshot: Optional[str] = None, **kwargs):
        """
        Creates a new Share under the account.

        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        """

        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.create_share(**kwargs)

    def delete_share(self, share_name: str, snapshot: Optional[str] = None,
                     delete_snapshot: Optional[bool] = False, **kwargs):
        """
        Marks the specified share for deletion. The share is later deleted
        during garbage collection.

        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param delete_snapshot: Indicates if snapshots are to be deleted.
        :type delete_snapshot: bool
        """
        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.delete_share(delete_snapshot=delete_snapshot, **kwargs)

    def create_snapshot(self, share_name: str, snapshot: Optional[str] = None, **kwargs):
        """
        Creates a snapshot of the share.

        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        """
        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.create_snapshot(**kwargs)

    def create_directory(self, directory_name, share_name: str,
                         snapshot: Optional[str] = None, **kwargs):
        """
        Creates a directory in the share and returns a client to interact with the directory.

        :param directory_name: The name of the directory
        :type directory_name: str
        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        """
        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.create_directory(directory_name, **kwargs)

    def delete_directory(self, directory_name, share_name: str,
                         snapshot: Optional[str] = None, **kwargs):
        """
        Marks the directory for deletion. The directory is later deleted during garbage collection.

        :param directory_name: The name of the directory
        :type directory_name: str
        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str

        """
        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.delete_directory(directory_name, **kwargs)

    def list_directories_and_files(self, share_name: str, snapshot: Optional[str] = None,
                                   directory_name: Optional[str] = None, **kwargs):
        """
         Lists the directories and files under the share.

        :param share_name: The name of the share with which to interact.
        :type share_name: str
        :param snapshot: An optional share snapshot on which to operate.
        :type snapshot: str
        :param directory_name:
        :param kwargs:
        :return:
        """

        share_client = self._get_share_client(share_name=share_name,
                                              snapshot=snapshot)
        return share_client.list_directories_and_files(
            directory_name=directory_name, **kwargs)

    def list_shares(self, name_starts_with=None, include_metadata=False,
                    include_snapshots=False, **kwargs):
        """
        List all shares in the account

        :param name_starts_with: Filters the results to return only
            shares whose names begin with the specified name_starts_with.
        :type name_starts_with: str

        :param include_metadata: Specifies that share metadata be returned in the response.
        :type include_metadata: Optional[bool]

        :param include_snapshots: Specifies that share snapshot be returned in the response.
        type include_snapshots: Optional[bool]

        """

        return self.get_conn().list_shares(name_starts_with=name_starts_with,
                                           include_metadata=include_metadata,
                                           include_snapshots=include_snapshots,
                                           **kwargs)
