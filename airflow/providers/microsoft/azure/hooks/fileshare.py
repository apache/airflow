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

import warnings
from typing import IO, Any

from azure.storage.file import File, FileService

from airflow.hooks.base import BaseHook


class AzureFileShareHook(BaseHook):
    """
    Interacts with Azure FileShare Storage.

    :param azure_fileshare_conn_id: Reference to the
        :ref:`Azure Container Volume connection id<howto/connection:azure_fileshare>`
        of an Azure account of which container volumes should be used.

    """

    conn_name_attr = "azure_fileshare_conn_id"
    default_conn_name = "azure_fileshare_default"
    conn_type = "azure_fileshare"
    hook_name = "Azure FileShare"

    def __init__(self, azure_fileshare_conn_id: str = "azure_fileshare_default") -> None:
        super().__init__()
        self.conn_id = azure_fileshare_conn_id
        self._conn = None

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "extra__azure_fileshare__sas_token": PasswordField(
                lazy_gettext("SAS Token (optional)"), widget=BS3PasswordFieldWidget()
            ),
            "extra__azure_fileshare__connection_string": StringField(
                lazy_gettext("Connection String (optional)"), widget=BS3TextFieldWidget()
            ),
            "extra__azure_fileshare__protocol": StringField(
                lazy_gettext("Account URL or token (optional)"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Blob Storage Login (optional)",
                "password": "Blob Storage Key (optional)",
            },
            "placeholders": {
                "login": "account name",
                "password": "secret",
                "extra__azure_fileshare__sas_token": "account url or token (optional)",
                "extra__azure_fileshare__connection_string": "account url or token (optional)",
                "extra__azure_fileshare__protocol": "account url or token (optional)",
            },
        }

    def get_conn(self) -> FileService:
        """Return the FileService object."""
        prefix = "extra__azure_fileshare__"
        if self._conn:
            return self._conn
        conn = self.get_connection(self.conn_id)
        service_options_with_prefix = conn.extra_dejson
        service_options = {}
        for key, value in service_options_with_prefix.items():
            # in case dedicated FileShareHook is used, the connection will use the extras from UI.
            # in case deprecated wasb hook is used, the old extras will work as well
            if key.startswith(prefix):
                if value != "":
                    service_options[key[len(prefix) :]] = value
                else:
                    # warn if the deprecated wasb_connection is used
                    warnings.warn(
                        "You are using deprecated connection for AzureFileShareHook."
                        " Please change it to `Azure FileShare`.",
                        DeprecationWarning,
                    )
            else:
                service_options[key] = value
                # warn if the old non-prefixed value is used
                warnings.warn(
                    "You are using deprecated connection for AzureFileShareHook."
                    " Please change it to `Azure FileShare`.",
                    DeprecationWarning,
                )
        self._conn = FileService(account_name=conn.login, account_key=conn.password, **service_options)
        return self._conn

    def check_for_directory(self, share_name: str, directory_name: str, **kwargs) -> bool:
        """
        Check if a directory exists on Azure File Share.

        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param kwargs: Optional keyword arguments that
            `FileService.exists()` takes.
        :return: True if the file exists, False otherwise.
        :rtype: bool
        """
        return self.get_conn().exists(share_name, directory_name, **kwargs)

    def check_for_file(self, share_name: str, directory_name: str, file_name: str, **kwargs) -> bool:
        """
        Check if a file exists on Azure File Share.

        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param file_name: Name of the file.
        :param kwargs: Optional keyword arguments that
            `FileService.exists()` takes.
        :return: True if the file exists, False otherwise.
        :rtype: bool
        """
        return self.get_conn().exists(share_name, directory_name, file_name, **kwargs)

    def list_directories_and_files(
        self, share_name: str, directory_name: str | None = None, **kwargs
    ) -> list:
        """
        Return the list of directories and files stored on a Azure File Share.

        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param kwargs: Optional keyword arguments that
            `FileService.list_directories_and_files()` takes.
        :return: A list of files and directories
        :rtype: list
        """
        return self.get_conn().list_directories_and_files(share_name, directory_name, **kwargs)

    def list_files(self, share_name: str, directory_name: str | None = None, **kwargs) -> list[str]:
        """
        Return the list of files stored on a Azure File Share.

        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param kwargs: Optional keyword arguments that
            `FileService.list_directories_and_files()` takes.
        :return: A list of files
        :rtype: list
        """
        return [
            obj.name
            for obj in self.list_directories_and_files(share_name, directory_name, **kwargs)
            if isinstance(obj, File)
        ]

    def create_share(self, share_name: str, **kwargs) -> bool:
        """
        Create new Azure File Share.

        :param share_name: Name of the share.
        :param kwargs: Optional keyword arguments that
            `FileService.create_share()` takes.
        :return: True if share is created, False if share already exists.
        :rtype: bool
        """
        return self.get_conn().create_share(share_name, **kwargs)

    def delete_share(self, share_name: str, **kwargs) -> bool:
        """
        Delete existing Azure File Share.

        :param share_name: Name of the share.
        :param kwargs: Optional keyword arguments that
            `FileService.delete_share()` takes.
        :return: True if share is deleted, False if share does not exist.
        :rtype: bool
        """
        return self.get_conn().delete_share(share_name, **kwargs)

    def create_directory(self, share_name: str, directory_name: str, **kwargs) -> list:
        """
        Create a new directory on a Azure File Share.

        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param kwargs: Optional keyword arguments that
            `FileService.create_directory()` takes.
        :return: A list of files and directories
        :rtype: list
        """
        return self.get_conn().create_directory(share_name, directory_name, **kwargs)

    def get_file(
        self, file_path: str, share_name: str, directory_name: str, file_name: str, **kwargs
    ) -> None:
        """
        Download a file from Azure File Share.

        :param file_path: Where to store the file.
        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param file_name: Name of the file.
        :param kwargs: Optional keyword arguments that
            `FileService.get_file_to_path()` takes.
        """
        self.get_conn().get_file_to_path(share_name, directory_name, file_name, file_path, **kwargs)

    def get_file_to_stream(
        self, stream: IO, share_name: str, directory_name: str, file_name: str, **kwargs
    ) -> None:
        """
        Download a file from Azure File Share.

        :param stream: A filehandle to store the file to.
        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param file_name: Name of the file.
        :param kwargs: Optional keyword arguments that
            `FileService.get_file_to_stream()` takes.
        """
        self.get_conn().get_file_to_stream(share_name, directory_name, file_name, stream, **kwargs)

    def load_file(
        self, file_path: str, share_name: str, directory_name: str, file_name: str, **kwargs
    ) -> None:
        """
        Upload a file to Azure File Share.

        :param file_path: Path to the file to load.
        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param file_name: Name of the file.
        :param kwargs: Optional keyword arguments that
            `FileService.create_file_from_path()` takes.
        """
        self.get_conn().create_file_from_path(share_name, directory_name, file_name, file_path, **kwargs)

    def load_string(
        self, string_data: str, share_name: str, directory_name: str, file_name: str, **kwargs
    ) -> None:
        """
        Upload a string to Azure File Share.

        :param string_data: String to load.
        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param file_name: Name of the file.
        :param kwargs: Optional keyword arguments that
            `FileService.create_file_from_text()` takes.
        """
        self.get_conn().create_file_from_text(share_name, directory_name, file_name, string_data, **kwargs)

    def load_stream(
        self, stream: str, share_name: str, directory_name: str, file_name: str, count: str, **kwargs
    ) -> None:
        """
        Upload a stream to Azure File Share.

        :param stream: Opened file/stream to upload as the file content.
        :param share_name: Name of the share.
        :param directory_name: Name of the directory.
        :param file_name: Name of the file.
        :param count: Size of the stream in bytes
        :param kwargs: Optional keyword arguments that
            `FileService.create_file_from_stream()` takes.
        """
        self.get_conn().create_file_from_stream(
            share_name, directory_name, file_name, stream, count, **kwargs
        )

    def test_connection(self):
        """Test Azure FileShare connection."""
        success = (True, "Successfully connected to Azure File Share.")

        try:
            # Attempt to retrieve file share information
            next(iter(self.get_conn().list_shares()))
            return success
        except StopIteration:
            # If the iterator returned is empty it should still be considered a successful connection since
            # it's possible to create a storage account without any file share and none could
            # legitimately exist yet.
            return success
        except Exception as e:
            return False, str(e)
