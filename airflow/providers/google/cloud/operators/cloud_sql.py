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
"""This module contains Google Cloud SQL operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLDatabaseHook, CloudSQLHook
from airflow.providers.google.cloud.links.cloud_sql import CloudSQLInstanceDatabaseLink, CloudSQLInstanceLink
from airflow.providers.google.cloud.utils.field_validator import GcpBodyFieldValidator
from airflow.providers.google.common.hooks.base_google import get_field
from airflow.providers.google.common.links.storage import FileDetailsLink
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


SETTINGS = 'settings'
SETTINGS_VERSION = 'settingsVersion'

CLOUD_SQL_CREATE_VALIDATION: Sequence[dict] = [
    dict(name="name", allow_empty=False),
    dict(
        name="settings",
        type="dict",
        fields=[
            dict(name="tier", allow_empty=False),
            dict(
                name="backupConfiguration",
                type="dict",
                fields=[
                    dict(name="binaryLogEnabled", optional=True),
                    dict(name="enabled", optional=True),
                    dict(name="replicationLogArchivingEnabled", optional=True),
                    dict(name="startTime", allow_empty=False, optional=True),
                ],
                optional=True,
            ),
            dict(name="activationPolicy", allow_empty=False, optional=True),
            dict(name="authorizedGaeApplications", type="list", optional=True),
            dict(name="crashSafeReplicationEnabled", optional=True),
            dict(name="dataDiskSizeGb", optional=True),
            dict(name="dataDiskType", allow_empty=False, optional=True),
            dict(name="databaseFlags", type="list", optional=True),
            dict(
                name="ipConfiguration",
                type="dict",
                fields=[
                    dict(
                        name="authorizedNetworks",
                        type="list",
                        fields=[
                            dict(name="expirationTime", optional=True),
                            dict(name="name", allow_empty=False, optional=True),
                            dict(name="value", allow_empty=False, optional=True),
                        ],
                        optional=True,
                    ),
                    dict(name="ipv4Enabled", optional=True),
                    dict(name="privateNetwork", allow_empty=False, optional=True),
                    dict(name="requireSsl", optional=True),
                ],
                optional=True,
            ),
            dict(
                name="locationPreference",
                type="dict",
                fields=[
                    dict(name="followGaeApplication", allow_empty=False, optional=True),
                    dict(name="zone", allow_empty=False, optional=True),
                ],
                optional=True,
            ),
            dict(
                name="maintenanceWindow",
                type="dict",
                fields=[
                    dict(name="hour", optional=True),
                    dict(name="day", optional=True),
                    dict(name="updateTrack", allow_empty=False, optional=True),
                ],
                optional=True,
            ),
            dict(name="pricingPlan", allow_empty=False, optional=True),
            dict(name="replicationType", allow_empty=False, optional=True),
            dict(name="storageAutoResize", optional=True),
            dict(name="storageAutoResizeLimit", optional=True),
            dict(name="userLabels", type="dict", optional=True),
        ],
    ),
    dict(name="databaseVersion", allow_empty=False, optional=True),
    dict(name="failoverReplica", type="dict", fields=[dict(name="name", allow_empty=False)], optional=True),
    dict(name="masterInstanceName", allow_empty=False, optional=True),
    dict(name="onPremisesConfiguration", type="dict", optional=True),
    dict(name="region", allow_empty=False, optional=True),
    dict(
        name="replicaConfiguration",
        type="dict",
        fields=[
            dict(name="failoverTarget", optional=True),
            dict(
                name="mysqlReplicaConfiguration",
                type="dict",
                fields=[
                    dict(name="caCertificate", allow_empty=False, optional=True),
                    dict(name="clientCertificate", allow_empty=False, optional=True),
                    dict(name="clientKey", allow_empty=False, optional=True),
                    dict(name="connectRetryInterval", optional=True),
                    dict(name="dumpFilePath", allow_empty=False, optional=True),
                    dict(name="masterHeartbeatPeriod", optional=True),
                    dict(name="password", allow_empty=False, optional=True),
                    dict(name="sslCipher", allow_empty=False, optional=True),
                    dict(name="username", allow_empty=False, optional=True),
                    dict(name="verifyServerCertificate", optional=True),
                ],
                optional=True,
            ),
        ],
        optional=True,
    ),
]
CLOUD_SQL_EXPORT_VALIDATION = [
    dict(
        name="exportContext",
        type="dict",
        fields=[
            dict(name="fileType", allow_empty=False),
            dict(name="uri", allow_empty=False),
            dict(name="databases", optional=True, type="list"),
            dict(
                name="sqlExportOptions",
                type="dict",
                optional=True,
                fields=[
                    dict(name="tables", optional=True, type="list"),
                    dict(name="schemaOnly", optional=True),
                    dict(
                        name="mysqlExportOptions",
                        type="dict",
                        optional=True,
                        fields=[dict(name="masterData")],
                    ),
                ],
            ),
            dict(
                name="csvExportOptions",
                type="dict",
                optional=True,
                fields=[
                    dict(name="selectQuery"),
                    dict(name="escapeCharacter", optional=True),
                    dict(name="quoteCharacter", optional=True),
                    dict(name="fieldsTerminatedBy", optional=True),
                    dict(name="linesTerminatedBy", optional=True),
                ],
            ),
            dict(name="offload", optional=True),
        ],
    )
]
CLOUD_SQL_IMPORT_VALIDATION = [
    dict(
        name="importContext",
        type="dict",
        fields=[
            dict(name="fileType", allow_empty=False),
            dict(name="uri", allow_empty=False),
            dict(name="database", optional=True, allow_empty=False),
            dict(name="importUser", optional=True),
            dict(
                name="csvImportOptions",
                type="dict",
                optional=True,
                fields=[dict(name="table"), dict(name="columns", type="list", optional=True)],
            ),
        ],
    )
]
CLOUD_SQL_DATABASE_CREATE_VALIDATION = [
    dict(name="instance", allow_empty=False),
    dict(name="name", allow_empty=False),
    dict(name="project", allow_empty=False),
]
CLOUD_SQL_DATABASE_PATCH_VALIDATION = [
    dict(name="instance", optional=True),
    dict(name="name", optional=True),
    dict(name="project", optional=True),
    dict(name="etag", optional=True),
    dict(name="charset", optional=True),
    dict(name="collation", optional=True),
]


class CloudSQLBaseOperator(BaseOperator):
    """
    Abstract base operator for Google Cloud SQL operators to inherit from.

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param project_id: Optional, Google Cloud Project ID.  f set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        *,
        instance: str,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.instance = instance
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance:
            raise AirflowException("The required parameter 'instance' is empty or None")

    def _check_if_instance_exists(self, instance, hook: CloudSQLHook) -> dict | bool:
        try:
            return hook.get_instance(project_id=self.project_id, instance=instance)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                return False
            raise e

    def _check_if_db_exists(self, db_name, hook: CloudSQLHook) -> dict | bool:
        try:
            return hook.get_database(project_id=self.project_id, instance=self.instance, database=db_name)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                return False
            raise e

    def execute(self, context: Context):
        pass

    @staticmethod
    def _get_settings_version(instance):
        return instance.get(SETTINGS).get(SETTINGS_VERSION)


class CloudSQLCreateInstanceOperator(CloudSQLBaseOperator):
    """
    Creates a new Cloud SQL instance.
    If an instance with the same name exists, no action will be taken and
    the operator will succeed.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLCreateInstanceOperator`

    :param body: Body required by the Cloud SQL insert API, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert
        #request-body
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param validate_body: True if body should be validated, False otherwise.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_create_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'body',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_create_template_fields]
    ui_color = '#FADBDA'
    operator_extra_links = (CloudSQLInstanceLink(),)

    def __init__(
        self,
        *,
        body: dict,
        instance: str,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self) -> None:
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_CREATE_VALIDATION, api_version=self.api_version).validate(
                self.body
            )

    def execute(self, context: Context) -> None:
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_body_fields()
        if not self._check_if_instance_exists(self.instance, hook):
            hook.create_instance(project_id=self.project_id, body=self.body)
        else:
            self.log.info("Cloud SQL instance with ID %s already exists. Aborting create.", self.instance)

        CloudSQLInstanceLink.persist(
            context=context,
            task_instance=self,
            cloud_sql_instance=self.instance,
            project_id=self.project_id or hook.project_id,
        )

        instance_resource = hook.get_instance(project_id=self.project_id, instance=self.instance)
        service_account_email = instance_resource["serviceAccountEmailAddress"]
        task_instance = context['task_instance']
        task_instance.xcom_push(key="service_account_email", value=service_account_email)


class CloudSQLInstancePatchOperator(CloudSQLBaseOperator):
    """
    Updates settings of a Cloud SQL instance.

    Caution: This is a partial update, so only included values for the settings will be
    updated.

    In the request body, supply the relevant portions of an instance resource, according
    to the rules of patch semantics.
    https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLInstancePatchOperator`

    :param body: Body required by the Cloud SQL patch API, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch#request-body
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param project_id: Optional, Google Cloud Project ID.  If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_patch_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'body',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_patch_template_fields]
    ui_color = '#FBDAC8'
    operator_extra_links = (CloudSQLInstanceLink(),)

    def __init__(
        self,
        *,
        body: dict,
        instance: str,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def execute(self, context: Context):
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        if not self._check_if_instance_exists(self.instance, hook):
            raise AirflowException(
                f'Cloud SQL instance with ID {self.instance} does not exist. '
                'Please specify another instance to patch.'
            )
        else:
            CloudSQLInstanceLink.persist(
                context=context,
                task_instance=self,
                cloud_sql_instance=self.instance,
                project_id=self.project_id or hook.project_id,
            )

            return hook.patch_instance(project_id=self.project_id, body=self.body, instance=self.instance)


class CloudSQLDeleteInstanceOperator(CloudSQLBaseOperator):
    """
    Deletes a Cloud SQL instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLDeleteInstanceOperator`

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_delete_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_delete_template_fields]
    ui_color = '#FEECD2'

    def execute(self, context: Context) -> bool | None:
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        if not self._check_if_instance_exists(self.instance, hook):
            print(f"Cloud SQL instance with ID {self.instance} does not exist. Aborting delete.")
            return True
        else:
            return hook.delete_instance(project_id=self.project_id, instance=self.instance)


class CloudSQLCreateInstanceDatabaseOperator(CloudSQLBaseOperator):
    """
    Creates a new database inside a Cloud SQL instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLCreateInstanceDatabaseOperator`

    :param instance: Database instance ID. This does not include the project ID.
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert#request-body
    :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param validate_body: Whether the body should be validated. Defaults to True.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_db_create_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'body',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_db_create_template_fields]
    ui_color = '#FFFCDB'
    operator_extra_links = (CloudSQLInstanceDatabaseLink(),)

    def __init__(
        self,
        *,
        instance: str,
        body: dict,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self) -> None:
        if self.validate_body:
            GcpBodyFieldValidator(
                CLOUD_SQL_DATABASE_CREATE_VALIDATION, api_version=self.api_version
            ).validate(self.body)

    def execute(self, context: Context) -> bool | None:
        self._validate_body_fields()
        database = self.body.get("name")
        if not database:
            self.log.error(
                "Body doesn't contain 'name'. Cannot check if the"
                " database already exists in the instance %s.",
                self.instance,
            )
            return False
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        CloudSQLInstanceDatabaseLink.persist(
            context=context,
            task_instance=self,
            cloud_sql_instance=self.instance,
            project_id=self.project_id or hook.project_id,
        )
        if self._check_if_db_exists(database, hook):
            self.log.info(
                "Cloud SQL instance with ID %s already contains database '%s'. Aborting database insert.",
                self.instance,
                database,
            )
            return True
        else:
            return hook.create_database(project_id=self.project_id, instance=self.instance, body=self.body)


class CloudSQLPatchInstanceDatabaseOperator(CloudSQLBaseOperator):
    """
    Updates a resource containing information about a database inside a Cloud SQL
    instance using patch semantics.
    See: https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLPatchInstanceDatabaseOperator`

    :param instance: Database instance ID. This does not include the project ID.
    :param database: Name of the database to be updated in the instance.
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/patch#request-body
    :param project_id: Optional, Google Cloud Project ID.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param validate_body: Whether the body should be validated. Defaults to True.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_db_patch_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'body',
        'database',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_db_patch_template_fields]
    ui_color = '#ECF4D9'
    operator_extra_links = (CloudSQLInstanceDatabaseLink(),)

    def __init__(
        self,
        *,
        instance: str,
        database: str,
        body: dict,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.database = database
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")
        if not self.database:
            raise AirflowException("The required parameter 'database' is empty")

    def _validate_body_fields(self) -> None:
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_DATABASE_PATCH_VALIDATION, api_version=self.api_version).validate(
                self.body
            )

    def execute(self, context: Context) -> None:
        self._validate_body_fields()
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        if not self._check_if_db_exists(self.database, hook):
            raise AirflowException(
                f"Cloud SQL instance with ID {self.instance} does not contain database '{self.database}'. "
                "Please specify another database to patch."
            )
        else:
            CloudSQLInstanceDatabaseLink.persist(
                context=context,
                task_instance=self,
                cloud_sql_instance=self.instance,
                project_id=self.project_id or hook.project_id,
            )
            return hook.patch_database(
                project_id=self.project_id, instance=self.instance, database=self.database, body=self.body
            )


class CloudSQLDeleteInstanceDatabaseOperator(CloudSQLBaseOperator):
    """
    Deletes a database from a Cloud SQL instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLDeleteInstanceDatabaseOperator`

    :param instance: Database instance ID. This does not include the project ID.
    :param database: Name of the database to be deleted in the instance.
    :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_db_delete_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'database',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_db_delete_template_fields]
    ui_color = '#D5EAD8'

    def __init__(
        self,
        *,
        instance: str,
        database: str,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.database = database
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.database:
            raise AirflowException("The required parameter 'database' is empty")

    def execute(self, context: Context) -> bool | None:
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        if not self._check_if_db_exists(self.database, hook):
            print(
                f"Cloud SQL instance with ID {self.instance!r} does not contain database {self.database!r}. "
                f"Aborting database delete."
            )
            return True
        else:
            return hook.delete_database(
                project_id=self.project_id, instance=self.instance, database=self.database
            )


class CloudSQLExportInstanceOperator(CloudSQLBaseOperator):
    """
    Exports data from a Cloud SQL instance to a Cloud Storage bucket as a SQL dump
    or CSV file.

    Note: This operator is idempotent. If executed multiple times with the same
    export file URI, the export file in GCS will simply be overridden.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLExportInstanceOperator`

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
    :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param validate_body: Whether the body should be validated. Defaults to True.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_export_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'body',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_export_template_fields]
    ui_color = '#D4ECEA'
    operator_extra_links = (CloudSQLInstanceLink(), FileDetailsLink())

    def __init__(
        self,
        *,
        instance: str,
        body: dict,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self) -> None:
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_EXPORT_VALIDATION, api_version=self.api_version).validate(
                self.body
            )

    def execute(self, context: Context) -> None:
        self._validate_body_fields()
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        CloudSQLInstanceLink.persist(
            context=context,
            task_instance=self,
            cloud_sql_instance=self.instance,
            project_id=self.project_id or hook.project_id,
        )
        FileDetailsLink.persist(
            context=context,
            task_instance=self,
            uri=self.body["exportContext"]["uri"][5:],
            project_id=self.project_id or hook.project_id,
        )
        return hook.export_instance(project_id=self.project_id, instance=self.instance, body=self.body)


class CloudSQLImportInstanceOperator(CloudSQLBaseOperator):
    """
    Imports data into a Cloud SQL instance from a SQL dump or CSV file in Cloud Storage.

    CSV IMPORT:

    This operator is NOT idempotent for a CSV import. If the same file is imported
    multiple times, the imported data will be duplicated in the database.
    Moreover, if there are any unique constraints the duplicate import may result in an
    error.

    SQL IMPORT:

    This operator is idempotent for a SQL import if it was also exported by Cloud SQL.
    The exported SQL contains 'DROP TABLE IF EXISTS' statements for all tables
    to be imported.

    If the import file was generated in a different way, idempotence is not guaranteed.
    It has to be ensured on the SQL file level.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLImportInstanceOperator`

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/import#request-body
    :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version: API version used (e.g. v1beta4).
    :param validate_body: Whether the body should be validated. Defaults to True.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_sql_import_template_fields]
    template_fields: Sequence[str] = (
        'project_id',
        'instance',
        'body',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcp_sql_import_template_fields]
    ui_color = '#D3EDFB'
    operator_extra_links = (CloudSQLInstanceLink(), FileDetailsLink())

    def __init__(
        self,
        *,
        instance: str,
        body: dict,
        project_id: str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self) -> None:
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_IMPORT_VALIDATION, api_version=self.api_version).validate(
                self.body
            )

    def execute(self, context: Context) -> None:
        self._validate_body_fields()
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        CloudSQLInstanceLink.persist(
            context=context,
            task_instance=self,
            cloud_sql_instance=self.instance,
            project_id=self.project_id or hook.project_id,
        )
        FileDetailsLink.persist(
            context=context,
            task_instance=self,
            uri=self.body["importContext"]["uri"][5:],
            project_id=self.project_id or hook.project_id,
        )
        return hook.import_instance(project_id=self.project_id, instance=self.instance, body=self.body)


class CloudSQLExecuteQueryOperator(BaseOperator):
    """
    Performs DML or DDL query on an existing Cloud Sql instance. It optionally uses
    cloud-sql-proxy to establish secure connection with the database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSQLExecuteQueryOperator`

    :param sql: SQL query or list of queries to run (should be DML or DDL query -
        this operator does not return any data from the database,
        so it is useless to pass it DQL queries. Note that it is responsibility of the
        author of the queries to make sure that the queries are idempotent. For example
        you can use CREATE TABLE IF NOT EXISTS to create a table.
    :param parameters: (optional) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param gcp_conn_id: The connection ID used to connect to Google Cloud for
        cloud-sql-proxy authentication.
    :param gcp_cloudsql_conn_id: The connection ID used to connect to Google Cloud SQL
       its schema should be gcpcloudsql://.
       See :class:`~airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook` for
       details on how to define ``gcpcloudsql://`` connection.
    """

    # [START gcp_sql_query_template_fields]
    template_fields: Sequence[str] = ('sql', 'gcp_cloudsql_conn_id', 'gcp_conn_id')
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    # [END gcp_sql_query_template_fields]
    ui_color = '#D3DEF1'

    def __init__(
        self,
        *,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        gcp_cloudsql_conn_id: str = 'google_cloud_sql_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.gcp_cloudsql_conn_id = gcp_cloudsql_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.gcp_connection: Connection | None = None

    def _execute_query(self, hook: CloudSQLDatabaseHook, database_hook: PostgresHook | MySqlHook) -> None:
        cloud_sql_proxy_runner = None
        try:
            if hook.use_proxy:
                cloud_sql_proxy_runner = hook.get_sqlproxy_runner()
                hook.free_reserved_port()
                # There is very, very slim chance that the socket will
                # be taken over here by another bind(0).
                # It's quite unlikely to happen though!
                cloud_sql_proxy_runner.start_proxy()
            self.log.info('Executing: "%s"', self.sql)
            database_hook.run(self.sql, self.autocommit, parameters=self.parameters)
        finally:
            if cloud_sql_proxy_runner:
                cloud_sql_proxy_runner.stop_proxy()

    def execute(self, context: Context):
        self.gcp_connection = BaseHook.get_connection(self.gcp_conn_id)
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id=self.gcp_cloudsql_conn_id,
            gcp_conn_id=self.gcp_conn_id,
            default_gcp_project_id=get_field(self.gcp_connection.extra_dejson, 'project'),
        )
        hook.validate_ssl_certs()
        connection = hook.create_connection()
        hook.validate_socket_path_length()
        database_hook = hook.get_database_hook(connection=connection)
        try:
            self._execute_query(hook, database_hook)
        finally:
            hook.cleanup_database_hook()
