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
from functools import wraps
from typing import IO, Any

from azure.storage.file import File, FileService

from airflow.hooks.base import BaseHook


def _ensure_prefixes(conn_type):
    """
    Remove when provider min airflow version >= 2.5.0 since this is handled by
    provider manager from that version.
    """

    def dec(func):
        @wraps(func)
        def inner():
            field_behaviors = func()
            conn_attrs = {"host", "schema", "login", "password", "port", "extra"}

            def _ensure_prefix(field):
                if field not in conn_attrs and not field.startswith("extra__"):
                    return f"extra__{conn_type}__{field}"
                else:
                    return field

            if "placeholders" in field_behaviors:
                placeholders = field_behaviors["placeholders"]
                field_behaviors["placeholders"] = {_ensure_prefix(k): v for k, v in placeholders.items()}
            return field_behaviors

        return inner

    return dec


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
            "sas_token": PasswordField(lazy_gettext("SAS Token (optional)"), widget=BS3PasswordFieldWidget()),
            "connection_string": StringField(
                lazy_gettext("Connection String (optional)"), widget=BS3TextFieldWidget()
            ),
            "protocol": StringField(
                lazy_gettext("Account URL or token (optional)"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    @_ensure_prefixes(conn_type="azure_fileshare")
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
                "sas_token": "account url or token (optional)",
                "connection_string": "account url or token (optional)",
                "protocol": "account url or token (optional)",
            },
        }

    def get_conn(self) -> FileService:
        """Return the FileService object."""

        def check_for_conflict(key):
            backcompat_key = f"{backcompat_prefix}{key}"
            if backcompat_key in extras:
                warnings.warn(
                    f"Conflicting params `{key}` and `{backcompat_key}` found in extras for conn "
                    f"{self.conn_id}. Using value for `{key}`.  Please ensure this is the correct value "
                    f"and remove the backcompat key `{backcompat_key}`."
                )

        backcompat_prefix = "extra__azure_fileshare__"
        if self._conn:
            return self._conn
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        service_options = {}
        for key, value in extras.items():
            if value == "":
                continue
            if not key.startswith("extra__"):
                service_options[key] = value
                check_for_conflict(key)
            elif key.startswith(backcompat_prefix):
                short_name = key[len(backcompat_prefix) :]
                if short_name not in service_options:  # prefer values provided with short name
                    service_options[short_name] = value
            else:
                warnings.warn(f"Extra param `{key}` not recognized; ignoring.")
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
        """
        return self.get_conn().create_share(share_name, **kwargs)

    def delete_share(self, share_name: str, **kwargs) -> bool:
        """
        Delete existing Azure File Share.

        :param share_name: Name of the share.
        :param kwargs: Optional keyword arguments that
            `FileService.delete_share()` takes.
        :return: True if share is deleted, False if share does not exist.
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
