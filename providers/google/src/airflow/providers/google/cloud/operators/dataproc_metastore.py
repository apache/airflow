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
"""This module contains Google Dataproc Metastore operators."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry, exponential_sleep_generator
from google.cloud.metastore_v1 import MetadataExport, MetadataManagementActivity
from google.cloud.metastore_v1.types import Backup, MetadataImport, Service
from google.cloud.metastore_v1.types.metastore import DatabaseDumpSpec, Restore

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.providers.google.cloud.hooks.dataproc_metastore import DataprocMetastoreHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.links.storage import StorageLink

if TYPE_CHECKING:
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


BASE_LINK = "https://console.cloud.google.com"
METASTORE_BASE_LINK = BASE_LINK + "/dataproc/metastore/services/{region}/{service_id}"
METASTORE_BACKUP_LINK = METASTORE_BASE_LINK + "/backups/{resource}?project={project_id}"
METASTORE_BACKUPS_LINK = METASTORE_BASE_LINK + "/backuprestore?project={project_id}"
METASTORE_EXPORT_LINK = METASTORE_BASE_LINK + "/importexport?project={project_id}"
METASTORE_IMPORT_LINK = METASTORE_BASE_LINK + "/imports/{resource}?project={project_id}"
METASTORE_SERVICE_LINK = METASTORE_BASE_LINK + "/config?project={project_id}"


class DataprocMetastoreLink(BaseOperatorLink):
    """Helper class for constructing Dataproc Metastore resource link."""

    name = "Dataproc Metastore"
    key = "conf"

    @staticmethod
    def persist(
        context: Context,
        task_instance: (
            DataprocMetastoreCreateServiceOperator
            | DataprocMetastoreGetServiceOperator
            | DataprocMetastoreRestoreServiceOperator
            | DataprocMetastoreUpdateServiceOperator
            | DataprocMetastoreListBackupsOperator
            | DataprocMetastoreExportMetadataOperator
        ),
        url: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataprocMetastoreLink.key,
            value={
                "region": task_instance.region,
                "service_id": task_instance.service_id,
                "project_id": task_instance.project_id,
                "url": url,
            },
        )

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        return (
            conf["url"].format(
                region=conf["region"],
                service_id=conf["service_id"],
                project_id=conf["project_id"],
            )
            if conf
            else ""
        )


class DataprocMetastoreDetailedLink(BaseOperatorLink):
    """Helper class for constructing Dataproc Metastore detailed resource link."""

    name = "Dataproc Metastore resource"
    key = "config"

    @staticmethod
    def persist(
        context: Context,
        task_instance: (
            DataprocMetastoreCreateBackupOperator | DataprocMetastoreCreateMetadataImportOperator
        ),
        url: str,
        resource: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataprocMetastoreDetailedLink.key,
            value={
                "region": task_instance.region,
                "service_id": task_instance.service_id,
                "project_id": task_instance.project_id,
                "url": url,
                "resource": resource,
            },
        )

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        return (
            conf["url"].format(
                region=conf["region"],
                service_id=conf["service_id"],
                project_id=conf["project_id"],
                resource=conf["resource"],
            )
            if conf
            else ""
        )


class DataprocMetastoreCreateBackupOperator(GoogleCloudBaseOperator):
    """
    Create a new backup in a given project and location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param backup:  Required. The backup to create. The ``name`` field is ignored. The ID of the created
        backup must be provided in the request's ``backup_id`` field.

        This corresponds to the ``backup`` field on the ``request`` instance; if ``request`` is provided, this
        should not be set.
    :param backup_id:  Required. The ID of the backup, which is used as the final component of the backup's
        name. This value must be between 1 and 64 characters long, begin with a letter, end with a letter or
        number, and consist of alphanumeric ASCII characters or hyphens.

        This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :param request_id: Optional. A unique id used to identify the request.
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "backup",
        "impersonation_chain",
    )
    template_fields_renderers = {"backup": "json"}
    operator_extra_links = (DataprocMetastoreDetailedLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        backup: dict | Backup,
        backup_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_id = service_id
        self.backup = backup
        self.backup_id = backup_id
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Creating Dataproc Metastore backup: %s", self.backup_id)

        try:
            operation = hook.create_backup(
                project_id=self.project_id,
                region=self.region,
                service_id=self.service_id,
                backup=self.backup,
                backup_id=self.backup_id,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            backup = hook.wait_for_operation(self.timeout, operation)
            self.log.info("Backup %s created successfully", self.backup_id)
        except AlreadyExists:
            self.log.info("Backup %s already exists", self.backup_id)
            backup = hook.get_backup(
                project_id=self.project_id,
                region=self.region,
                service_id=self.service_id,
                backup_id=self.backup_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        DataprocMetastoreDetailedLink.persist(
            context=context, task_instance=self, url=METASTORE_BACKUP_LINK, resource=self.backup_id
        )
        return Backup.to_dict(backup)


class DataprocMetastoreCreateMetadataImportOperator(GoogleCloudBaseOperator):
    """
    Create a new MetadataImport in a given project and location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param metadata_import:  Required. The metadata import to create. The ``name`` field is ignored. The ID of
        the created metadata import must be provided in the request's ``metadata_import_id`` field.

        This corresponds to the ``metadata_import`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param metadata_import_id:  Required. The ID of the metadata import, which is used as the final component
        of the metadata import's name. This value must be between 1 and 64 characters long, begin with a
        letter, end with a letter or number, and consist of alphanumeric ASCII characters or hyphens.

        This corresponds to the ``metadata_import_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param request_id: Optional. A unique id used to identify the request.
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "metadata_import",
        "impersonation_chain",
    )
    template_fields_renderers = {"metadata_import": "json"}
    operator_extra_links = (DataprocMetastoreDetailedLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        metadata_import: dict | MetadataImport,
        metadata_import_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_id = service_id
        self.metadata_import = metadata_import
        self.metadata_import_id = metadata_import_id
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Creating Dataproc Metastore metadata import: %s", self.metadata_import_id)
        operation = hook.create_metadata_import(
            project_id=self.project_id,
            region=self.region,
            service_id=self.service_id,
            metadata_import=self.metadata_import,
            metadata_import_id=self.metadata_import_id,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        metadata_import = hook.wait_for_operation(self.timeout, operation)
        self.log.info("Metadata import %s created successfully", self.metadata_import_id)

        DataprocMetastoreDetailedLink.persist(
            context=context, task_instance=self, url=METASTORE_IMPORT_LINK, resource=self.metadata_import_id
        )
        return MetadataImport.to_dict(metadata_import)


class DataprocMetastoreCreateServiceOperator(GoogleCloudBaseOperator):
    """
    Create a metastore service in a project and location.

    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param service:  Required. The Metastore service to create. The ``name`` field is ignored. The ID of
        the created metastore service must be provided in the request's ``service_id`` field.

        This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param request_id: Optional. A unique id used to identify the request.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "service",
        "impersonation_chain",
    )
    template_fields_renderers = {"service": "json"}
    operator_extra_links = (DataprocMetastoreLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        service: dict | Service,
        service_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.service = service
        self.service_id = service_id
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Creating Dataproc Metastore service: %s", self.service_id)
        try:
            operation = hook.create_service(
                region=self.region,
                project_id=self.project_id,
                service=self.service,
                service_id=self.service_id,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            service = hook.wait_for_operation(self.timeout, operation)
            self.log.info("Service %s created successfully", self.service_id)
        except AlreadyExists:
            self.log.info("Instance %s already exists", self.service_id)
            service = hook.get_service(
                region=self.region,
                project_id=self.project_id,
                service_id=self.service_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        DataprocMetastoreLink.persist(context=context, task_instance=self, url=METASTORE_SERVICE_LINK)
        return Service.to_dict(service)


class DataprocMetastoreDeleteBackupOperator(GoogleCloudBaseOperator):
    """
    Delete a single backup.

    :param project_id: Required. The ID of the Google Cloud project that the backup belongs to.
    :param region: Required. The ID of the Google Cloud region that the backup belongs to.
    :param service_id: Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param backup_id:  Required. The ID of the backup, which is used as the final component of the backup's
        name. This value must be between 1 and 64 characters long, begin with a letter, end with a letter or
        number, and consist of alphanumeric ASCII characters or hyphens.

        This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :param request_id: Optional. A unique id used to identify the request.
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        backup_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_id = service_id
        self.backup_id = backup_id
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Deleting Dataproc Metastore backup: %s", self.backup_id)
        operation = hook.delete_backup(
            project_id=self.project_id,
            region=self.region,
            service_id=self.service_id,
            backup_id=self.backup_id,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(self.timeout, operation)
        self.log.info("Backup %s deleted successfully", self.project_id)


class DataprocMetastoreDeleteServiceOperator(GoogleCloudBaseOperator):
    """
    Delete a single service.

    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        service_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.service_id = service_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Deleting Dataproc Metastore service: %s", self.service_id)
        operation = hook.delete_service(
            region=self.region,
            project_id=self.project_id,
            service_id=self.service_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(self.timeout, operation)
        self.log.info("Service %s deleted successfully", self.service_id)


class DataprocMetastoreExportMetadataOperator(GoogleCloudBaseOperator):
    """
    Export metadata from a service.

    :param destination_gcs_folder: A Cloud Storage URI of a folder, in the format
        ``gs://<bucket_name>/<path_inside_bucket>``. A sub-folder
        ``<export_folder>`` containing exported files will be
        created below it.
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.
        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param request_id: Optional. A unique id used to identify the request.
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocMetastoreLink(), StorageLink())

    def __init__(
        self,
        *,
        destination_gcs_folder: str,
        project_id: str,
        region: str,
        service_id: str,
        request_id: str | None = None,
        database_dump_type: DatabaseDumpSpec | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.destination_gcs_folder = destination_gcs_folder
        self.project_id = project_id
        self.region = region
        self.service_id = service_id
        self.request_id = request_id
        self.database_dump_type = database_dump_type
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Exporting metadata from Dataproc Metastore service: %s", self.service_id)
        hook.export_metadata(
            destination_gcs_folder=self.destination_gcs_folder,
            project_id=self.project_id,
            region=self.region,
            service_id=self.service_id,
            request_id=self.request_id,
            database_dump_type=self.database_dump_type,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        metadata_export = self._wait_for_export_metadata(hook)
        self.log.info("Metadata from service %s exported successfully", self.service_id)

        DataprocMetastoreLink.persist(context=context, task_instance=self, url=METASTORE_EXPORT_LINK)
        uri = self._get_uri_from_destination(MetadataExport.to_dict(metadata_export)["destination_gcs_uri"])
        StorageLink.persist(context=context, task_instance=self, uri=uri, project_id=self.project_id)
        return MetadataExport.to_dict(metadata_export)

    def _get_uri_from_destination(self, destination_uri: str):
        return destination_uri[5:] if destination_uri.startswith("gs://") else destination_uri

    def _wait_for_export_metadata(self, hook: DataprocMetastoreHook):
        """
        Check that export was created successfully.

        This is a workaround to an issue parsing result to MetadataExport inside
        the SDK.
        """
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            time.sleep(time_to_wait)
            service = hook.get_service(
                region=self.region,
                project_id=self.project_id,
                service_id=self.service_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            activities: MetadataManagementActivity = service.metadata_management_activity
            metadata_export: MetadataExport = activities.metadata_exports[0]
            if metadata_export.state == MetadataExport.State.SUCCEEDED:
                return metadata_export
            if metadata_export.state == MetadataExport.State.FAILED:
                raise AirflowException(
                    f"Exporting metadata from Dataproc Metastore {metadata_export.name} FAILED"
                )


class DataprocMetastoreGetServiceOperator(GoogleCloudBaseOperator):
    """
    Get the details of a single service.

    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocMetastoreLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        service_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.service_id = service_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Gets the details of a single Dataproc Metastore service: %s", self.project_id)
        result = hook.get_service(
            region=self.region,
            project_id=self.project_id,
            service_id=self.service_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataprocMetastoreLink.persist(context=context, task_instance=self, url=METASTORE_SERVICE_LINK)
        return Service.to_dict(result)


class DataprocMetastoreListBackupsOperator(GoogleCloudBaseOperator):
    """
    List backups in a service.

    :param project_id: Required. The ID of the Google Cloud project that the backup belongs to.
    :param region: Required. The ID of the Google Cloud region that the backup belongs to.
    :param service_id: Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocMetastoreLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_id = service_id
        self.page_size = page_size
        self.page_token = page_token
        self.filter = filter
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list[dict]:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Listing Dataproc Metastore backups: %s", self.service_id)
        backups = hook.list_backups(
            project_id=self.project_id,
            region=self.region,
            service_id=self.service_id,
            page_size=self.page_size,
            page_token=self.page_token,
            filter=self.filter,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataprocMetastoreLink.persist(context=context, task_instance=self, url=METASTORE_BACKUPS_LINK)
        return [Backup.to_dict(backup) for backup in backups]


class DataprocMetastoreRestoreServiceOperator(GoogleCloudBaseOperator):
    """
    Restore a service from a backup.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_id: Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param backup_project_id: Required. The ID of the Google Cloud project that the metastore
        service backup to restore from.
    :param backup_region: Required. The ID of the Google Cloud region that the metastore
        service backup to restore from.
    :param backup_service_id:  Required. The ID of the metastore service backup to restore from, which is
        used as the final component of the metastore service's name. This value must be between 2 and 63
        characters long inclusive, begin with a letter, end with a letter or number, and consist
        of alphanumeric ASCII characters or hyphens.
    :param backup_id:  Required. The ID of the metastore service backup to restore from
    :param restore_type: Optional. The type of restore. If unspecified, defaults to
        ``METADATA_ONLY``
    :param request_id: Optional. A unique id used to identify the request.
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocMetastoreLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        backup_project_id: str,
        backup_region: str,
        backup_service_id: str,
        backup_id: str,
        restore_type: Restore | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_id = service_id
        self.backup_project_id = backup_project_id
        self.backup_region = backup_region
        self.backup_service_id = backup_service_id
        self.backup_id = backup_id
        self.restore_type = restore_type
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info(
            "Restoring Dataproc Metastore service: %s from backup: %s", self.service_id, self.backup_id
        )
        hook.restore_service(
            project_id=self.project_id,
            region=self.region,
            service_id=self.service_id,
            backup_project_id=self.backup_project_id,
            backup_region=self.backup_region,
            backup_service_id=self.backup_service_id,
            backup_id=self.backup_id,
            restore_type=self.restore_type,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self._wait_for_restore_service(hook)
        self.log.info("Service %s restored from backup %s", self.service_id, self.backup_id)
        DataprocMetastoreLink.persist(context=context, task_instance=self, url=METASTORE_SERVICE_LINK)

    def _wait_for_restore_service(self, hook: DataprocMetastoreHook):
        """
        Check that export was created successfully.

        This is a workaround to an issue parsing result to MetadataExport inside
        the SDK.
        """
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            time.sleep(time_to_wait)
            service = hook.get_service(
                region=self.region,
                project_id=self.project_id,
                service_id=self.service_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            activities: MetadataManagementActivity = service.metadata_management_activity
            restore_service: Restore = activities.restores[0]
            if restore_service.state == Restore.State.SUCCEEDED:
                return restore_service
            if restore_service.state == Restore.State.FAILED:
                raise AirflowException("Restoring service FAILED")


class DataprocMetastoreUpdateServiceOperator(GoogleCloudBaseOperator):
    """
    Update the parameters of a single service.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :param service:  Required. The metastore service to update. The server only merges fields in the service
        if they are specified in ``update_mask``.

        The metastore service's ``name`` field is used to identify the metastore service to be updated.

        This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :param update_mask:  Required. A field mask used to specify the fields to be overwritten in the metastore
        service resource by the update. Fields specified in the ``update_mask`` are relative to the resource
        (not to the full request). A field is overwritten if it is in the mask.

        This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :param request_id: Optional. A unique id used to identify the request.
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocMetastoreLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        service: dict | Service,
        update_mask: FieldMask,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_id = service_id
        self.service = service
        self.update_mask = update_mask
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Updating Dataproc Metastore service: %s", self.service.get("name"))

        operation = hook.update_service(
            project_id=self.project_id,
            region=self.region,
            service_id=self.service_id,
            service=self.service,
            update_mask=self.update_mask,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(self.timeout, operation)
        self.log.info("Service %s updated successfully", self.service.get("name"))
        DataprocMetastoreLink.persist(context=context, task_instance=self, url=METASTORE_SERVICE_LINK)
