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
This module contains integration with Azure Blob Storage.

It communicate via the Window Azure Storage Blob protocol. Make sure that a
Airflow connection of type `wasb` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection `wasb_default` for an example).
"""
from __future__ import annotations

import logging
import os
from functools import wraps
from typing import Any, Union

from asgiref.sync import sync_to_async
from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.identity.aio import (
    ClientSecretCredential as AsyncClientSecretCredential,
    DefaultAzureCredential as AsyncDefaultAzureCredential,
)
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient, StorageStreamDownloader
from azure.storage.blob._models import BlobProperties
from azure.storage.blob.aio import (
    BlobClient as AsyncBlobClient,
    BlobServiceClient as AsyncBlobServiceClient,
    ContainerClient as AsyncContainerClient,
)

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

AsyncCredentials = Union[AsyncClientSecretCredential, AsyncDefaultAzureCredential]


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


class WasbHook(BaseHook):
    """
    Interacts with Azure Blob Storage through the ``wasb://`` protocol.

    These parameters have to be passed in Airflow Data Base: account_name and account_key.

    Additional options passed in the 'extra' field of the connection will be
    passed to the `BlockBlockService()` constructor. For example, authenticate
    using a SAS token by adding {"sas_token": "YOUR_TOKEN"}.

    If no authentication configuration is provided, DefaultAzureCredential will be used (applicable
    when using Azure compute infrastructure).

    :param wasb_conn_id: Reference to the :ref:`wasb connection <howto/connection:wasb>`.
    :param public_read: Whether an anonymous public read access should be used. default is False
    """

    conn_name_attr = "wasb_conn_id"
    default_conn_name = "wasb_default"
    conn_type = "wasb"
    hook_name = "Azure Blob Storage"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "connection_string": PasswordField(
                lazy_gettext("Blob Storage Connection String (optional)"), widget=BS3PasswordFieldWidget()
            ),
            "shared_access_key": PasswordField(
                lazy_gettext("Blob Storage Shared Access Key (optional)"), widget=BS3PasswordFieldWidget()
            ),
            "tenant_id": StringField(
                lazy_gettext("Tenant Id (Active Directory Auth)"), widget=BS3TextFieldWidget()
            ),
            "sas_token": PasswordField(lazy_gettext("SAS Token (optional)"), widget=BS3PasswordFieldWidget()),
        }

    @staticmethod
    @_ensure_prefixes(conn_type="wasb")
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "login": "Blob Storage Login (optional)",
                "password": "Blob Storage Key (optional)",
                "host": "Account Name (Active Directory Auth)",
            },
            "placeholders": {
                "login": "account name",
                "password": "secret",
                "host": "account url",
                "connection_string": "connection string auth",
                "tenant_id": "tenant",
                "shared_access_key": "shared access key",
                "sas_token": "account url or token",
            },
        }

    def __init__(
        self,
        wasb_conn_id: str = default_conn_name,
        public_read: bool = False,
    ) -> None:
        super().__init__()
        self.conn_id = wasb_conn_id
        self.public_read = public_read
        self.blob_service_client = self.get_conn()

        logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
        try:
            logger.setLevel(os.environ.get("AZURE_HTTP_LOGGING_LEVEL", logging.WARNING))
        except ValueError:
            logger.setLevel(logging.WARNING)

    def _get_field(self, extra_dict, field_name):
        prefix = "extra__wasb__"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{prefix}' prefix "
                f"when using this method."
            )
        if field_name in extra_dict:
            return extra_dict[field_name] or None
        return extra_dict.get(f"{prefix}{field_name}") or None

    def get_conn(self) -> BlobServiceClient:
        """Return the BlobServiceClient object."""
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        if self.public_read:
            # Here we use anonymous public read
            # more info
            # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-manage-access-to-resources
            return BlobServiceClient(account_url=conn.host, **extra)

        connection_string = self._get_field(extra, "connection_string")
        if connection_string:
            # connection_string auth takes priority
            return BlobServiceClient.from_connection_string(connection_string, **extra)

        shared_access_key = self._get_field(extra, "shared_access_key")
        if shared_access_key:
            # using shared access key
            return BlobServiceClient(account_url=conn.host, credential=shared_access_key, **extra)

        tenant = self._get_field(extra, "tenant_id")
        if tenant:
            # use Active Directory auth
            app_id = conn.login
            app_secret = conn.password
            token_credential = ClientSecretCredential(tenant, app_id, app_secret)
            return BlobServiceClient(account_url=conn.host, credential=token_credential, **extra)

        sas_token = self._get_field(extra, "sas_token")
        if sas_token:
            if sas_token.startswith("https"):
                return BlobServiceClient(account_url=sas_token, **extra)
            else:
                return BlobServiceClient(
                    account_url=f"https://{conn.login}.blob.core.windows.net/{sas_token}", **extra
                )

        # Fall back to old auth (password) or use managed identity if not provided.
        credential = conn.password
        if not credential:
            credential = DefaultAzureCredential()
            self.log.info("Using DefaultAzureCredential as credential")
        return BlobServiceClient(
            account_url=f"https://{conn.login}.blob.core.windows.net/",
            credential=credential,
            **extra,
        )

    def _get_container_client(self, container_name: str) -> ContainerClient:
        """
        Instantiates a container client

        :param container_name: The name of the container
        :return: ContainerClient
        """
        return self.blob_service_client.get_container_client(container_name)

    def _get_blob_client(self, container_name: str, blob_name: str) -> BlobClient:
        """
        Instantiates a blob client

        :param container_name: The name of the blob container
        :param blob_name: The name of the blob. This needs not be existing
        """
        return self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    def check_for_blob(self, container_name: str, blob_name: str, **kwargs) -> bool:
        """
        Check if a blob exists on Azure Blob Storage.

        :param container_name: Name of the container.
        :param blob_name: Name of the blob.
        :param kwargs: Optional keyword arguments for ``BlobClient.get_blob_properties`` takes.
        :return: True if the blob exists, False otherwise.
        """
        try:
            self._get_blob_client(container_name, blob_name).get_blob_properties(**kwargs)
        except ResourceNotFoundError:
            return False
        return True

    def check_for_prefix(self, container_name: str, prefix: str, **kwargs) -> bool:
        """
        Check if a prefix exists on Azure Blob storage.

        :param container_name: Name of the container.
        :param prefix: Prefix of the blob.
        :param kwargs: Optional keyword arguments that ``ContainerClient.walk_blobs`` takes
        :return: True if blobs matching the prefix exist, False otherwise.
        """
        blobs = self.get_blobs_list(container_name=container_name, prefix=prefix, **kwargs)
        return len(blobs) > 0

    def get_blobs_list(
        self,
        container_name: str,
        prefix: str | None = None,
        include: list[str] | None = None,
        delimiter: str = "/",
        **kwargs,
    ) -> list:
        """
        List blobs in a given container

        :param container_name: The name of the container
        :param prefix: Filters the results to return only blobs whose names
            begin with the specified prefix.
        :param include: Specifies one or more additional datasets to include in the
            response. Options include: ``snapshots``, ``metadata``, ``uncommittedblobs``,
            ``copy`, ``deleted``.
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        """
        container = self._get_container_client(container_name)
        blob_list = []
        blobs = container.walk_blobs(name_starts_with=prefix, include=include, delimiter=delimiter, **kwargs)
        for blob in blobs:
            blob_list.append(blob.name)
        return blob_list

    def load_file(
        self,
        file_path: str,
        container_name: str,
        blob_name: str,
        create_container: bool = False,
        **kwargs,
    ) -> None:
        """
        Upload a file to Azure Blob Storage.

        :param file_path: Path to the file to load.
        :param container_name: Name of the container.
        :param blob_name: Name of the blob.
        :param create_container: Attempt to create the target container prior to uploading the blob. This is
            useful if the target container may not exist yet. Defaults to False.
        :param kwargs: Optional keyword arguments that ``BlobClient.upload_blob()`` takes.
        """
        with open(file_path, "rb") as data:
            self.upload(
                container_name=container_name,
                blob_name=blob_name,
                data=data,
                create_container=create_container,
                **kwargs,
            )

    def load_string(
        self,
        string_data: str,
        container_name: str,
        blob_name: str,
        create_container: bool = False,
        **kwargs,
    ) -> None:
        """
        Upload a string to Azure Blob Storage.

        :param string_data: String to load.
        :param container_name: Name of the container.
        :param blob_name: Name of the blob.
        :param create_container: Attempt to create the target container prior to uploading the blob. This is
            useful if the target container may not exist yet. Defaults to False.
        :param kwargs: Optional keyword arguments that ``BlobClient.upload()`` takes.
        """
        # Reorder the argument order from airflow.providers.amazon.aws.hooks.s3.load_string.
        self.upload(
            container_name=container_name,
            blob_name=blob_name,
            data=string_data,
            create_container=create_container,
            **kwargs,
        )

    def get_file(self, file_path: str, container_name: str, blob_name: str, **kwargs):
        """
        Download a file from Azure Blob Storage.

        :param file_path: Path to the file to download.
        :param container_name: Name of the container.
        :param blob_name: Name of the blob.
        :param kwargs: Optional keyword arguments that `BlobClient.download_blob()` takes.
        """
        with open(file_path, "wb") as fileblob:
            stream = self.download(container_name=container_name, blob_name=blob_name, **kwargs)
            fileblob.write(stream.readall())

    def read_file(self, container_name: str, blob_name: str, **kwargs):
        """
        Read a file from Azure Blob Storage and return as a string.

        :param container_name: Name of the container.
        :param blob_name: Name of the blob.
        :param kwargs: Optional keyword arguments that `BlobClient.download_blob` takes.
        """
        return self.download(container_name, blob_name, **kwargs).content_as_text()

    def upload(
        self,
        container_name: str,
        blob_name: str,
        data: Any,
        blob_type: str = "BlockBlob",
        length: int | None = None,
        create_container: bool = False,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Creates a new blob from a data source with automatic chunking.

        :param container_name: The name of the container to upload data
        :param blob_name: The name of the blob to upload. This need not exist in the container
        :param data: The blob data to upload
        :param blob_type: The type of the blob. This can be either ``BlockBlob``,
            ``PageBlob`` or ``AppendBlob``. The default value is ``BlockBlob``.
        :param length: Number of bytes to read from the stream. This is optional,
            but should be supplied for optimal performance.
        :param create_container: Attempt to create the target container prior to uploading the blob. This is
            useful if the target container may not exist yet. Defaults to False.
        """
        if create_container:
            self.create_container(container_name)

        blob_client = self._get_blob_client(container_name, blob_name)
        return blob_client.upload_blob(data, blob_type, length=length, **kwargs)

    def download(
        self, container_name, blob_name, offset: int | None = None, length: int | None = None, **kwargs
    ) -> StorageStreamDownloader:
        """
        Downloads a blob to the StorageStreamDownloader

        :param container_name: The name of the container containing the blob
        :param blob_name: The name of the blob to download
        :param offset: Start of byte range to use for downloading a section of the blob.
            Must be set if length is provided.
        :param length: Number of bytes to read from the stream.
        """
        blob_client = self._get_blob_client(container_name, blob_name)
        return blob_client.download_blob(offset=offset, length=length, **kwargs)

    def create_container(self, container_name: str) -> None:
        """
        Create container object if not already existing

        :param container_name: The name of the container to create
        """
        container_client = self._get_container_client(container_name)
        try:
            self.log.debug("Attempting to create container: %s", container_name)
            container_client.create_container()
            self.log.info("Created container: %s", container_name)
        except ResourceExistsError:
            self.log.info(
                "Attempted to create container %r but it already exists. If it is expected that this "
                "container will always exist, consider setting create_container to False.",
                container_name,
            )
        except HttpResponseError as e:
            self.log.info(
                "Received an HTTP response error while attempting to creating container %r: %s"
                "\nIf the error is related to missing permissions to create containers, please consider "
                "setting create_container to False or supplying connection credentials with the "
                "appropriate permission for connection ID %r.",
                container_name,
                e.response,
                self.conn_id,
            )
        except Exception as e:
            self.log.info("Error while attempting to create container %r: %s", container_name, e)
            raise

    def delete_container(self, container_name: str) -> None:
        """
        Delete a container object

        :param container_name: The name of the container
        """
        try:
            self.log.debug("Attempting to delete container: %s", container_name)
            self._get_container_client(container_name).delete_container()
            self.log.info("Deleted container: %s", container_name)
        except ResourceNotFoundError:
            self.log.info("Unable to delete container %s (not found)", container_name)
        except:
            self.log.info("Error deleting container: %s", container_name)
            raise

    def delete_blobs(self, container_name: str, *blobs, **kwargs) -> None:
        """
        Marks the specified blobs or snapshots for deletion.

        :param container_name: The name of the container containing the blobs
        :param blobs: The blobs to delete. This can be a single blob, or multiple values
            can be supplied, where each value is either the name of the blob (str) or BlobProperties.
        """
        self._get_container_client(container_name).delete_blobs(*blobs, **kwargs)
        self.log.info("Deleted blobs: %s", blobs)

    def delete_file(
        self,
        container_name: str,
        blob_name: str,
        is_prefix: bool = False,
        ignore_if_missing: bool = False,
        delimiter: str = "",
        **kwargs,
    ) -> None:
        """
        Delete a file, or all blobs matching a prefix, from Azure Blob Storage.

        :param container_name: Name of the container.
        :param blob_name: Name of the blob.
        :param is_prefix: If blob_name is a prefix, delete all matching files
        :param ignore_if_missing: if True, then return success even if the
            blob does not exist.
        :param kwargs: Optional keyword arguments that ``ContainerClient.delete_blobs()`` takes.
        """
        if is_prefix:
            blobs_to_delete = self.get_blobs_list(
                container_name, prefix=blob_name, delimiter=delimiter, **kwargs
            )
        elif self.check_for_blob(container_name, blob_name):
            blobs_to_delete = [blob_name]
        else:
            blobs_to_delete = []
        if not ignore_if_missing and len(blobs_to_delete) == 0:
            raise AirflowException(f"Blob(s) not found: {blob_name}")

        # The maximum number of blobs that can be deleted in a single request is 256 using the underlying
        # `ContainerClient.delete_blobs()` method. Therefore the deletes need to be in batches of <= 256.
        num_blobs_to_delete = len(blobs_to_delete)
        for i in range(0, num_blobs_to_delete, 256):
            self.delete_blobs(container_name, *blobs_to_delete[i : i + 256], **kwargs)

    def test_connection(self):
        """Test Azure Blob Storage connection."""
        success = (True, "Successfully connected to Azure Blob Storage.")

        try:
            # Attempt to retrieve storage account information
            self.get_conn().get_account_information()
            return success
        except Exception as e:
            return False, str(e)


class WasbAsyncHook(WasbHook):
    """
    An async hook that connects to Azure WASB to perform operations.

    :param wasb_conn_id: reference to the :ref:`wasb connection <howto/connection:wasb>`
    :param public_read: whether an anonymous public read access should be used. default is False
    """

    def __init__(
        self,
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
    ) -> None:
        """Initialize the hook instance."""
        self.conn_id = wasb_conn_id
        self.public_read = public_read
        self.blob_service_client: AsyncBlobServiceClient = None  # type: ignore

    async def get_async_conn(self) -> AsyncBlobServiceClient:
        """Return the Async BlobServiceClient object."""
        if self.blob_service_client is not None:
            return self.blob_service_client

        conn = await sync_to_async(self.get_connection)(self.conn_id)
        extra = conn.extra_dejson or {}

        if self.public_read:
            # Here we use anonymous public read
            # more info
            # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-manage-access-to-resources
            self.blob_service_client = AsyncBlobServiceClient(account_url=conn.host, **extra)
            return self.blob_service_client

        connection_string = self._get_field(extra, "connection_string")
        if connection_string:
            # connection_string auth takes priority
            self.blob_service_client = AsyncBlobServiceClient.from_connection_string(
                connection_string, **extra
            )
            return self.blob_service_client

        shared_access_key = self._get_field(extra, "shared_access_key")
        if shared_access_key:
            # using shared access key
            self.blob_service_client = AsyncBlobServiceClient(
                account_url=conn.host, credential=shared_access_key, **extra
            )
            return self.blob_service_client

        tenant = self._get_field(extra, "tenant_id")
        if tenant:
            # use Active Directory auth
            app_id = conn.login
            app_secret = conn.password
            token_credential = AsyncClientSecretCredential(tenant, app_id, app_secret)
            self.blob_service_client = AsyncBlobServiceClient(
                account_url=conn.host, credential=token_credential, **extra  # type:ignore[arg-type]
            )
            return self.blob_service_client

        sas_token = self._get_field(extra, "sas_token")
        if sas_token:
            if sas_token.startswith("https"):
                self.blob_service_client = AsyncBlobServiceClient(account_url=sas_token, **extra)
            else:
                self.blob_service_client = AsyncBlobServiceClient(
                    account_url=f"https://{conn.login}.blob.core.windows.net/{sas_token}", **extra
                )
            return self.blob_service_client

        # Fall back to old auth (password) or use managed identity if not provided.
        credential = conn.password
        if not credential:
            credential = AsyncDefaultAzureCredential()
            self.log.info("Using DefaultAzureCredential as credential")
        self.blob_service_client = AsyncBlobServiceClient(
            account_url=f"https://{conn.login}.blob.core.windows.net/",
            credential=credential,
            **extra,
        )

        return self.blob_service_client

    def _get_blob_client(self, container_name: str, blob_name: str) -> AsyncBlobClient:
        """
        Instantiate a blob client.

        :param container_name: the name of the blob container
        :param blob_name: the name of the blob. This needs not be existing
        """
        return self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    async def check_for_blob_async(self, container_name: str, blob_name: str, **kwargs: Any) -> bool:
        """
        Check if a blob exists on Azure Blob Storage.

        :param container_name: name of the container
        :param blob_name: name of the blob
        :param kwargs: optional keyword arguments for ``BlobClient.get_blob_properties``
        """
        try:
            await self._get_blob_client(container_name, blob_name).get_blob_properties(**kwargs)
        except ResourceNotFoundError:
            return False
        return True

    def _get_container_client(self, container_name: str) -> AsyncContainerClient:
        """
        Instantiate a container client.

        :param container_name: the name of the container
        """
        return self.blob_service_client.get_container_client(container_name)

    async def get_blobs_list_async(
        self,
        container_name: str,
        prefix: str | None = None,
        include: list[str] | None = None,
        delimiter: str = "/",
        **kwargs: Any,
    ) -> list[BlobProperties]:
        """
        List blobs in a given container.

        :param container_name: the name of the container
        :param prefix: filters the results to return only blobs whose names
            begin with the specified prefix.
        :param include: specifies one or more additional datasets to include in the
            response. Options include: ``snapshots``, ``metadata``, ``uncommittedblobs``,
            ``copy`, ``deleted``.
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        """
        container = self._get_container_client(container_name)
        blob_list = []
        blobs = container.walk_blobs(name_starts_with=prefix, include=include, delimiter=delimiter, **kwargs)
        async for blob in blobs:
            blob_list.append(blob.name)
        return blob_list

    async def check_for_prefix_async(self, container_name: str, prefix: str, **kwargs: Any) -> bool:
        """
        Check if a prefix exists on Azure Blob storage.

        :param container_name: Name of the container.
        :param prefix: Prefix of the blob.
        :param kwargs: Optional keyword arguments for ``ContainerClient.walk_blobs``
        """
        blobs = await self.get_blobs_list_async(container_name=container_name, prefix=prefix, **kwargs)
        return len(blobs) > 0
