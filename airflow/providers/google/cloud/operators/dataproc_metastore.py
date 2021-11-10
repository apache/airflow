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
"""This module contains Google Dataproc Metastore operators."""

from time import sleep
from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry, exponential_sleep_generator
from google.cloud.metastore_v1 import MetadataExport, MetadataManagementActivity
from google.cloud.metastore_v1.types import Backup, MetadataImport, Service
from google.cloud.metastore_v1.types.metastore import DatabaseDumpSpec, Restore
from google.protobuf.field_mask_pb2 import FieldMask
from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.dataproc_metastore import DataprocMetastoreHook


class DataprocMetastoreCreateBackupOperator(BaseOperator):
    """
    Creates a new backup in a given project and location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param backup:  Required. The backup to create. The ``name`` field is ignored. The ID of the created
        backup must be provided in the request's ``backup_id`` field.

        This corresponds to the ``backup`` field on the ``request`` instance; if ``request`` is provided, this
        should not be set.
    :type backup: google.cloud.metastore_v1.types.Backup
    :param backup_id:  Required. The ID of the backup, which is used as the final component of the backup's
        name. This value must be between 1 and 64 characters long, begin with a letter, end with a letter or
        number, and consist of alphanumeric ASCII characters or hyphens.

        This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type backup_id: str
    :param request_id: Optional. A unique id used to identify the request.
    :type request_id: str
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: Optional. The timeout for this request.
    :type timeout: float
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'backup',
        'impersonation_chain',
    )
    template_fields_renderers = {'backup': 'json'}

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        backup: Union[Dict, Backup],
        backup_id: str,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: dict) -> dict:
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
        except HttpError as err:
            if err.resp.status not in (409, '409'):
                raise
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
        return Backup.to_dict(backup)


class DataprocMetastoreCreateMetadataImportOperator(BaseOperator):
    """
    Creates a new MetadataImport in a given project and location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param metadata_import:  Required. The metadata import to create. The ``name`` field is ignored. The ID of
        the created metadata import must be provided in the request's ``metadata_import_id`` field.

        This corresponds to the ``metadata_import`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type metadata_import: google.cloud.metastore_v1.types.MetadataImport
    :param metadata_import_id:  Required. The ID of the metadata import, which is used as the final component
        of the metadata import's name. This value must be between 1 and 64 characters long, begin with a
        letter, end with a letter or number, and consist of alphanumeric ASCII characters or hyphens.

        This corresponds to the ``metadata_import_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type metadata_import_id: str
    :param request_id: Optional. A unique id used to identify the request.
    :type request_id: str
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: Optional. The timeout for this request.
    :type timeout: float
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'metadata_import',
        'impersonation_chain',
    )
    template_fields_renderers = {'metadata_import': 'json'}

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        metadata_import: MetadataImport,
        metadata_import_id: str,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: dict):
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
        return MetadataImport.to_dict(metadata_import)


class DataprocMetastoreCreateServiceOperator(BaseOperator):
    """
    Creates a metastore service in a project and location.

    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param service:  Required. The Metastore service to create. The ``name`` field is ignored. The ID of
        the created metastore service must be provided in the request's ``service_id`` field.

        This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type service: google.cloud.metastore_v1.types.Service
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param request_id: Optional. A unique id used to identify the request.
    :type request_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'service',
        'impersonation_chain',
    )
    template_fields_renderers = {'service': 'json'}

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        service: Optional[Union[Dict, Service]] = None,
        service_id: str,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context) -> dict:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Creating Dataproc Metastore service: %s", self.project_id)
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
        except HttpError as err:
            if err.resp.status not in (409, '409'):
                raise
            self.log.info("Instance %s already exists", self.service_id)
            service = hook.get_service(
                region=self.region,
                project_id=self.project_id,
                service_id=self.service_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        return Service.to_dict(service)


class DataprocMetastoreDeleteBackupOperator(BaseOperator):
    """
    Deletes a single backup.

    :param project_id: Required. The ID of the Google Cloud project that the backup belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the backup belongs to.
    :type region: str
    :param service_id: Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param backup_id:  Required. The ID of the backup, which is used as the final component of the backup's
        name. This value must be between 1 and 64 characters long, begin with a letter, end with a letter or
        number, and consist of alphanumeric ASCII characters or hyphens.

        This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type backup_id: str
    :param request_id: Optional. A unique id used to identify the request.
    :type request_id: str
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: Optional. The timeout for this request.
    :type timeout: float
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        backup_id: str,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: dict) -> None:
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


class DataprocMetastoreDeleteServiceOperator(BaseOperator):
    """
    Deletes a single service.

    :param request:  The request object. Request message for
        [DataprocMetastore.DeleteService][google.cloud.metastore.v1.DataprocMetastore.DeleteService].
    :type request: google.cloud.metastore_v1.types.DeleteServiceRequest
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    template_fields = (
        'project_id',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        service_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context) -> dict:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Deleting Dataproc Metastore service: %s", self.project_id)
        operation = hook.delete_service(
            region=self.region,
            project_id=self.project_id,
            service_id=self.service_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(self.timeout, operation)
        self.log.info("Service %s deleted successfully", self.project_id)


class DataprocMetastoreExportMetadataOperator(BaseOperator):
    """
    Exports metadata from a service.

    :param destination_gcs_folder: A Cloud Storage URI of a folder, in the format
        ``gs://<bucket_name>/<path_inside_bucket>``. A sub-folder
        ``<export_folder>`` containing exported files will be
        created below it.
    :type destination_gcs_folder: str
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.
        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param request_id: Optional. A unique id used to identify the request.
    :type request_id: str
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: Optional. The timeout for this request.
    :type timeout: float
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        destination_gcs_folder: str,
        project_id: str,
        region: str,
        service_id: str,
        request_id: Optional[str] = None,
        database_dump_type: Optional[DatabaseDumpSpec] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: Dict):
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
        return MetadataExport.to_dict(metadata_export)

    def _wait_for_export_metadata(self, hook: DataprocMetastoreHook):
        """
        Workaround to check that export was created successfully.
        We discovered a issue to parse result to MetadataExport inside the SDK
        """
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            sleep(time_to_wait)
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


class DataprocMetastoreGetServiceOperator(BaseOperator):
    """
    Gets the details of a single service.

    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        service_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context) -> dict:
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
        return Service.to_dict(result)


class DataprocMetastoreListBackupsOperator(BaseOperator):
    """
    Lists backups in a service.

    :param project_id: Required. The ID of the Google Cloud project that the backup belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the backup belongs to.
    :type region: str
    :param service_id: Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: Optional. The timeout for this request.
    :type timeout: float
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        filter: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: dict) -> dict:
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
        return [Backup.to_dict(backup) for backup in backups]


class DataprocMetastoreRestoreServiceOperator(BaseOperator):
    """
    Restores a service from a backup.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param service_id: Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param backup_project_id: Required. The ID of the Google Cloud project that the metastore
        service backup to restore from.
    :type backup_project_id: str
    :param backup_region: Required. The ID of the Google Cloud region that the metastore
        service backup to restore from.
    :type backup_region: str
    :param backup_service_id:  Required. The ID of the metastore service backup to restore from, which is
        used as the final component of the metastore service's name. This value must be between 2 and 63
        characters long inclusive, begin with a letter, end with a letter or number, and consist
        of alphanumeric ASCII characters or hyphens.
    :type backup_service_id: str
    :param backup_id:  Required. The ID of the metastore service backup to restore from
    :type backup_id: str
    :param restore_type: Optional. The type of restore. If unspecified, defaults to
        ``METADATA_ONLY``
    :type restore_type: google.cloud.metastore_v1.types.Restore.RestoreType
    :param request_id: Optional. A unique id used to identify the request.
    :type request_id: str
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: Optional. The timeout for this request.
    :type timeout: float
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'impersonation_chain',
    )

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
        restore_type: Optional[Restore] = None,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context) -> dict:
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

    def _wait_for_restore_service(self, hook: DataprocMetastoreHook):
        """
        Workaround to check that restore service was finished successfully.
        We discovered an issue to parse result to Restore inside the SDK
        """
        for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
            sleep(time_to_wait)
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


class DataprocMetastoreUpdateServiceOperator(BaseOperator):
    """
    Updates the parameters of a single service.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of
        the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
        with a letter, end with a letter or number, and consist of alphanumeric ASCII characters or
        hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type service_id: str
    :param service:  Required. The metastore service to update. The server only merges fields in the service
        if they are specified in ``update_mask``.

        The metastore service's ``name`` field is used to identify the metastore service to be updated.

        This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type service: Union[Dict, google.cloud.metastore_v1.types.Service]
    :param update_mask:  Required. A field mask used to specify the fields to be overwritten in the metastore
        service resource by the update. Fields specified in the ``update_mask`` are relative to the resource
        (not to the full request). A field is overwritten if it is in the mask.

        This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type update_mask: google.protobuf.field_mask_pb2.FieldMask
    :param request_id: Optional. A unique id used to identify the request.
    :type request_id: str
    :param retry: Optional. Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: Optional. The timeout for this request.
    :type timeout: float
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'project_id',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        service_id: str,
        service: Union[Dict, Service],
        update_mask: Union[Dict, FieldMask],
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: Dict):
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
