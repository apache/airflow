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

from airflow.hooks.base_hook import BaseHook
from azure.storage.file import FileService


class AzureFileShareHook(BaseHook):
    """
    Interacts with Azure FileShare Storage.

    Additional options passed in the 'extra' field of the connection will be
    passed to the `FileService()` constructor.

    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    """

    def __init__(self, wasb_conn_id='wasb_default'):
        self.conn_id = wasb_conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        """Return the FileService object."""
        conn = self.get_connection(self.conn_id)
        service_options = conn.extra_dejson
        return FileService(account_name=conn.login,
                           account_key=conn.password, **service_options)

    def check_for_directory(self, share_name, directory_name, **kwargs):
        """
        Check if a directory exists on Azure File Share.

        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.exists()` takes.
        :type kwargs: object
        :return: True if the file exists, False otherwise.
        :rtype bool
        """
        return self.connection.exists(share_name, directory_name,
                                      **kwargs)

    def check_for_file(self, share_name, directory_name, file_name, **kwargs):
        """
        Check if a file exists on Azure File Share.

        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param file_name: Name of the file.
        :type file_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.exists()` takes.
        :type kwargs: object
        :return: True if the file exists, False otherwise.
        :rtype bool
        """
        return self.connection.exists(share_name, directory_name,
                                      file_name, **kwargs)

    def list_directories_and_files(self, share_name, directory_name=None, **kwargs):
        """
        Return the list of directories and files stored on a Azure File Share.

        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.list_directories_and_files()` takes.
        :type kwargs: object
        :return: A list of files and directories
        :rtype list
        """
        return self.connection.list_directories_and_files(share_name,
                                                          directory_name,
                                                          **kwargs)

    def create_directory(self, share_name, directory_name, **kwargs):
        """
        Create a new direcotry on a Azure File Share.

        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.create_directory()` takes.
        :type kwargs: object
        :return: A list of files and directories
        :rtype list
        """
        return self.connection.create_directory(share_name, directory_name, **kwargs)

    def get_file(self, file_path, share_name, directory_name, file_name, **kwargs):
        """
        Download a file from Azure File Share.

        :param file_path: Where to store the file.
        :type file_path: str
        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param file_name: Name of the file.
        :type file_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.get_file_to_path()` takes.
        :type kwargs: object
        """
        self.connection.get_file_to_path(share_name, directory_name,
                                         file_name, file_path, **kwargs)

    def get_file_to_stream(self, stream, share_name, directory_name, file_name, **kwargs):
        """
        Download a file from Azure File Share.

        :param stream: A filehandle to store the file to.
        :type stream: file-like object
        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param file_name: Name of the file.
        :type file_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.get_file_to_stream()` takes.
        :type kwargs: object
        """
        self.connection.get_file_to_stream(share_name, directory_name,
                                           file_name, stream, **kwargs)

    def load_file(self, file_path, share_name, directory_name, file_name, **kwargs):
        """
        Upload a file to Azure File Share.

        :param file_path: Path to the file to load.
        :type file_path: str
        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param file_name: Name of the file.
        :type file_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.create_file_from_path()` takes.
        :type kwargs: object
        """
        self.connection.create_file_from_path(share_name, directory_name,
                                              file_name, file_path, **kwargs)

    def load_string(self, string_data, share_name, directory_name, file_name, **kwargs):
        """
        Upload a string to Azure File Share.

        :param string_data: String to load.
        :type string_data: str
        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param file_name: Name of the file.
        :type file_name: str
        :param kwargs: Optional keyword arguments that
            `FileService.create_file_from_text()` takes.
        :type kwargs: object
        """
        self.connection.create_file_from_text(share_name, directory_name,
                                              file_name, string_data, **kwargs)

    def load_stream(self, stream, share_name, directory_name, file_name, count, **kwargs):
        """
        Upload a stream to Azure File Share.

        :param stream: Opened file/stream to upload as the file content.
        :type stream: file-like
        :param share_name: Name of the share.
        :type share_name: str
        :param directory_name: Name of the directory.
        :type directory_name: str
        :param file_name: Name of the file.
        :type file_name: str
        :param count: Size of the stream in bytes
        :type count: int
        :param kwargs: Optional keyword arguments that
            `FileService.create_file_from_stream()` takes.
        :type kwargs: object
        """
        self.connection.create_file_from_stream(share_name, directory_name,
                                                file_name, stream, count, **kwargs)
