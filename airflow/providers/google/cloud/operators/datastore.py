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
This module contains Google Datastore operators.
"""
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.datastore import DatastoreHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class CloudDatastoreExportEntitiesOperator(BaseOperator):
    """
    Export entities from Google Cloud Datastore to Cloud Storage

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDatastoreExportEntitiesOperator`

    :param bucket: name of the cloud storage bucket to backup data
    :type bucket: str
    :param namespace: optional namespace path in the specified Cloud Storage bucket
        to backup data. If this namespace does not exist in GCS, it will be created.
    :type namespace: str
    :param datastore_conn_id: the name of the Datastore connection id to use
    :type datastore_conn_id: str
    :param cloud_storage_conn_id: the name of the cloud storage connection id to
        force-write backup
    :type cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param entity_filter: description of what data from the project is included in the
        export, refer to
        https://cloud.google.com/datastore/docs/reference/rest/Shared.Types/EntityFilter
    :type entity_filter: dict
    :param labels: client-assigned labels for cloud storage
    :type labels: dict
    :param polling_interval_in_seconds: number of seconds to wait before polling for
        execution status again
    :type polling_interval_in_seconds: int
    :param overwrite_existing: if the storage bucket + namespace is not empty, it will be
        emptied prior to exports. This enables overwriting existing backups.
    :type overwrite_existing: bool
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

    template_fields = [
        'bucket',
        'namespace',
        'entity_filter',
        'labels',
        'impersonation_chain',
    ]

    @apply_defaults
    def __init__(
        self,  # pylint: disable=too-many-arguments
        *,
        bucket: str,
        namespace: Optional[str] = None,
        datastore_conn_id: str = 'google_cloud_default',
        cloud_storage_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        entity_filter: Optional[dict] = None,
        labels: Optional[dict] = None,
        polling_interval_in_seconds: int = 10,
        overwrite_existing: bool = False,
        project_id: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.datastore_conn_id = datastore_conn_id
        self.cloud_storage_conn_id = cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.bucket = bucket
        self.namespace = namespace
        self.entity_filter = entity_filter
        self.labels = labels
        self.polling_interval_in_seconds = polling_interval_in_seconds
        self.overwrite_existing = overwrite_existing
        self.project_id = project_id
        self.impersonation_chain = impersonation_chain
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context) -> dict:
        self.log.info('Exporting data to Cloud Storage bucket %s', self.bucket)

        if self.overwrite_existing and self.namespace:
            gcs_hook = GCSHook(self.cloud_storage_conn_id, impersonation_chain=self.impersonation_chain)
            objects = gcs_hook.list(self.bucket, prefix=self.namespace)
            for obj in objects:
                gcs_hook.delete(self.bucket, obj)

        ds_hook = DatastoreHook(
            self.datastore_conn_id,
            self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = ds_hook.export_to_storage_bucket(
            bucket=self.bucket,
            namespace=self.namespace,
            entity_filter=self.entity_filter,
            labels=self.labels,
            project_id=self.project_id,
        )
        operation_name = result['name']
        result = ds_hook.poll_operation_until_done(operation_name, self.polling_interval_in_seconds)

        state = result['metadata']['common']['state']
        if state != 'SUCCESSFUL':
            raise AirflowException('Operation failed: result={}'.format(result))
        return result


class CloudDatastoreImportEntitiesOperator(BaseOperator):
    """
    Import entities from Cloud Storage to Google Cloud Datastore

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDatastoreImportEntitiesOperator`

    :param bucket: container in Cloud Storage to store data
    :type bucket: str
    :param file: path of the backup metadata file in the specified Cloud Storage bucket.
        It should have the extension .overall_export_metadata
    :type file: str
    :param namespace: optional namespace of the backup metadata file in
        the specified Cloud Storage bucket.
    :type namespace: str
    :param entity_filter: description of what data from the project is included in
        the export, refer to
        https://cloud.google.com/datastore/docs/reference/rest/Shared.Types/EntityFilter
    :type entity_filter: dict
    :param labels: client-assigned labels for cloud storage
    :type labels: dict
    :param datastore_conn_id: the name of the connection id to use
    :type datastore_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param polling_interval_in_seconds: number of seconds to wait before polling for
        execution status again
    :type polling_interval_in_seconds: float
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

    template_fields = [
        'bucket',
        'file',
        'namespace',
        'entity_filter',
        'labels',
        'impersonation_chain',
    ]

    @apply_defaults
    def __init__(
        self,
        *,
        bucket: str,
        file: str,
        namespace: Optional[str] = None,
        entity_filter: Optional[dict] = None,
        labels: Optional[dict] = None,
        datastore_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        polling_interval_in_seconds: float = 10,
        project_id: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.datastore_conn_id = datastore_conn_id
        self.delegate_to = delegate_to
        self.bucket = bucket
        self.file = file
        self.namespace = namespace
        self.entity_filter = entity_filter
        self.labels = labels
        self.polling_interval_in_seconds = polling_interval_in_seconds
        self.project_id = project_id
        self.impersonation_chain = impersonation_chain
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context):
        self.log.info('Importing data from Cloud Storage bucket %s', self.bucket)
        ds_hook = DatastoreHook(
            self.datastore_conn_id,
            self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = ds_hook.import_from_storage_bucket(
            bucket=self.bucket,
            file=self.file,
            namespace=self.namespace,
            entity_filter=self.entity_filter,
            labels=self.labels,
            project_id=self.project_id,
        )
        operation_name = result['name']
        result = ds_hook.poll_operation_until_done(operation_name, self.polling_interval_in_seconds)

        state = result['metadata']['common']['state']
        if state != 'SUCCESSFUL':
            raise AirflowException('Operation failed: result={}'.format(result))

        return result


class CloudDatastoreAllocateIdsOperator(BaseOperator):
    """
    Allocate IDs for incomplete keys. Return list of keys.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDatastoreAllocateIdsOperator`

    .. seealso::
        https://cloud.google.com/datastore/docs/reference/rest/v1/projects/allocateIds

    :param partial_keys: a list of partial keys.
    :type partial_keys: list
    :param project_id: Google Cloud project ID against which to make the request.
    :type project_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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
        "partial_keys",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        partial_keys: List,
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.partial_keys = partial_keys
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> list:
        hook = DatastoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        keys = hook.allocate_ids(
            partial_keys=self.partial_keys,
            project_id=self.project_id,
        )
        return keys


class CloudDatastoreBeginTransactionOperator(BaseOperator):
    """
    Begins a new transaction. Returns a transaction handle.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDatastoreBeginTransactionOperator`

    .. seealso::
        https://cloud.google.com/datastore/docs/reference/rest/v1/projects/beginTransaction

    :param transaction_options: Options for a new transaction.
    :type transaction_options: Dict[str, Any]
    :param project_id: Google Cloud project ID against which to make the request.
    :type project_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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
        "transaction_options",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        transaction_options: Dict[str, Any],
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.transaction_options = transaction_options
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> str:
        hook = DatastoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        handle = hook.begin_transaction(
            transaction_options=self.transaction_options,
            project_id=self.project_id,
        )
        return handle


class CloudDatastoreCommitOperator(BaseOperator):
    """
    Commit a transaction, optionally creating, deleting or modifying some entities.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDatastoreCommitOperator`

    .. seealso::
        https://cloud.google.com/datastore/docs/reference/rest/v1/projects/commit

    :param body: the body of the commit request.
    :type body: dict
    :param project_id: Google Cloud project ID against which to make the request.
    :type project_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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
        "body",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        body: Dict[str, Any],
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> dict:
        hook = DatastoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.commit(
            body=self.body,
            project_id=self.project_id,
        )
        return response


class CloudDatastoreRollbackOperator(BaseOperator):
    """
    Roll back a transaction.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDatastoreRollbackOperator`

    .. seealso::
        https://cloud.google.com/datastore/docs/reference/rest/v1/projects/rollback

    :param transaction: the transaction to roll back.
    :type transaction: str
    :param project_id: Google Cloud project ID against which to make the request.
    :type project_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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
        "transaction",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        transaction: str,
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.transaction = transaction
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> None:
        hook = DatastoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.rollback(
            transaction=self.transaction,
            project_id=self.project_id,
        )


class CloudDatastoreRunQueryOperator(BaseOperator):
    """
    Run a query for entities. Returns the batch of query results.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDatastoreRunQueryOperator`

    .. seealso::
        https://cloud.google.com/datastore/docs/reference/rest/v1/projects/runQuery

    :param body: the body of the query request.
    :type body: dict
    :param project_id: Google Cloud project ID against which to make the request.
    :type project_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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
        "body",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        body: Dict[str, Any],
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> dict:
        hook = DatastoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.run_query(
            body=self.body,
            project_id=self.project_id,
        )
        return response


class CloudDatastoreGetOperationOperator(BaseOperator):
    """
    Gets the latest state of a long-running operation.

    .. seealso::
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/get

    :param name: the name of the operation resource.
    :type name: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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
        "name",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        name: str,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.name = name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = DatastoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        op = hook.get_operation(name=self.name)
        return op


class CloudDatastoreDeleteOperationOperator(BaseOperator):
    """
    Deletes the long-running operation.

    .. seealso::
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/delete

    :param name: the name of the operation resource.
    :type name: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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
        "name",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        name: str,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.name = name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> None:
        hook = DatastoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_operation(name=self.name)
