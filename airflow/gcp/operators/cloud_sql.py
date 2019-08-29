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
"""
This module contains Google Cloud SQL operators.
"""
from typing import Dict, Iterable, List, Optional, Union

from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.gcp.hooks.cloud_sql import CloudSqlDatabaseHook, CloudSqlHook
from airflow.gcp.utils.field_validator import GcpBodyFieldValidator
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

SETTINGS = 'settings'
SETTINGS_VERSION = 'settingsVersion'

CLOUD_SQL_CREATE_VALIDATION = [
    dict(name="name", allow_empty=False),
    dict(name="settings", type="dict", fields=[
        dict(name="tier", allow_empty=False),
        dict(name="backupConfiguration", type="dict", fields=[
            dict(name="binaryLogEnabled", optional=True),
            dict(name="enabled", optional=True),
            dict(name="replicationLogArchivingEnabled", optional=True),
            dict(name="startTime", allow_empty=False, optional=True)
        ], optional=True),
        dict(name="activationPolicy", allow_empty=False, optional=True),
        dict(name="authorizedGaeApplications", type="list", optional=True),
        dict(name="crashSafeReplicationEnabled", optional=True),
        dict(name="dataDiskSizeGb", optional=True),
        dict(name="dataDiskType", allow_empty=False, optional=True),
        dict(name="databaseFlags", type="list", optional=True),
        dict(name="ipConfiguration", type="dict", fields=[
            dict(name="authorizedNetworks", type="list", fields=[
                dict(name="expirationTime", optional=True),
                dict(name="name", allow_empty=False, optional=True),
                dict(name="value", allow_empty=False, optional=True)
            ], optional=True),
            dict(name="ipv4Enabled", optional=True),
            dict(name="privateNetwork", allow_empty=False, optional=True),
            dict(name="requireSsl", optional=True),
        ], optional=True),
        dict(name="locationPreference", type="dict", fields=[
            dict(name="followGaeApplication", allow_empty=False, optional=True),
            dict(name="zone", allow_empty=False, optional=True),
        ], optional=True),
        dict(name="maintenanceWindow", type="dict", fields=[
            dict(name="hour", optional=True),
            dict(name="day", optional=True),
            dict(name="updateTrack", allow_empty=False, optional=True),
        ], optional=True),
        dict(name="pricingPlan", allow_empty=False, optional=True),
        dict(name="replicationType", allow_empty=False, optional=True),
        dict(name="storageAutoResize", optional=True),
        dict(name="storageAutoResizeLimit", optional=True),
        dict(name="userLabels", type="dict", optional=True),
    ]),
    dict(name="databaseVersion", allow_empty=False, optional=True),
    dict(name="failoverReplica", type="dict", fields=[
        dict(name="name", allow_empty=False)
    ], optional=True),
    dict(name="masterInstanceName", allow_empty=False, optional=True),
    dict(name="onPremisesConfiguration", type="dict", optional=True),
    dict(name="region", allow_empty=False, optional=True),
    dict(name="replicaConfiguration", type="dict", fields=[
        dict(name="failoverTarget", optional=True),
        dict(name="mysqlReplicaConfiguration", type="dict", fields=[
            dict(name="caCertificate", allow_empty=False, optional=True),
            dict(name="clientCertificate", allow_empty=False, optional=True),
            dict(name="clientKey", allow_empty=False, optional=True),
            dict(name="connectRetryInterval", optional=True),
            dict(name="dumpFilePath", allow_empty=False, optional=True),
            dict(name="masterHeartbeatPeriod", optional=True),
            dict(name="password", allow_empty=False, optional=True),
            dict(name="sslCipher", allow_empty=False, optional=True),
            dict(name="username", allow_empty=False, optional=True),
            dict(name="verifyServerCertificate", optional=True)
        ], optional=True),
    ], optional=True)
]
CLOUD_SQL_EXPORT_VALIDATION = [
    dict(name="exportContext", type="dict", fields=[
        dict(name="fileType", allow_empty=False),
        dict(name="uri", allow_empty=False),
        dict(name="databases", optional=True, type="list"),
        dict(name="sqlExportOptions", type="dict", optional=True, fields=[
            dict(name="tables", optional=True, type="list"),
            dict(name="schemaOnly", optional=True)
        ]),
        dict(name="csvExportOptions", type="dict", optional=True, fields=[
            dict(name="selectQuery")
        ])
    ])
]
CLOUD_SQL_IMPORT_VALIDATION = [
    dict(name="importContext", type="dict", fields=[
        dict(name="fileType", allow_empty=False),
        dict(name="uri", allow_empty=False),
        dict(name="database", optional=True, allow_empty=False),
        dict(name="importUser", optional=True),
        dict(name="csvImportOptions", type="dict", optional=True, fields=[
            dict(name="table"),
            dict(name="columns", type="list", optional=True)
        ])
    ])
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


class CloudSqlBaseOperator(BaseOperator):
    """
    Abstract base operator for Google Cloud SQL operators to inherit from.

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param project_id: Optional, Google Cloud Platform Project ID.  f set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    """
    @apply_defaults
    def __init__(self,
                 instance: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 *args, **kwargs) -> None:
        self.project_id = project_id
        self.instance = instance
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self._hook = CloudSqlHook(gcp_conn_id=self.gcp_conn_id,
                                  api_version=self.api_version)  # type: CloudSqlHook
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance:
            raise AirflowException("The required parameter 'instance' is empty or None")

    def _check_if_instance_exists(self, instance):
        try:
            return self._hook.get_instance(project_id=self.project_id,
                                           instance=instance)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                return False
            raise e

    def _check_if_db_exists(self, db_name):
        try:
            return self._hook.get_database(
                project_id=self.project_id,
                instance=self.instance,
                database=db_name)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                return False
            raise e

    def execute(self, context):
        pass

    @staticmethod
    def _get_settings_version(instance):
        return instance.get(SETTINGS).get(SETTINGS_VERSION)


class CloudSqlInstanceCreateOperator(CloudSqlBaseOperator):
    """
    Creates a new Cloud SQL instance.
    If an instance with the same name exists, no action will be taken and
    the operator will succeed.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlInstanceCreateOperator`

    :param body: Body required by the Cloud SQL insert API, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert
        #request-body
    :type body: dict
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param project_id: Optional, Google Cloud Platform Project ID. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    :param validate_body: True if body should be validated, False otherwise.
    :type validate_body: bool
    """
    # [START gcp_sql_create_template_fields]
    template_fields = ('project_id', 'instance', 'body', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_create_template_fields]

    @apply_defaults
    def __init__(self,
                 body: dict,
                 instance: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 validate_body: bool = True,
                 *args, **kwargs) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self):
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_CREATE_VALIDATION,
                                  api_version=self.api_version).validate(self.body)

    def execute(self, context):
        self._validate_body_fields()
        if not self._check_if_instance_exists(self.instance):
            self._hook.create_instance(
                project_id=self.project_id,
                body=self.body)
        else:
            self.log.info("Cloud SQL instance with ID %s already exists. "
                          "Aborting create.", self.instance)

        instance_resource = self._hook.get_instance(project_id=self.project_id,
                                                    instance=self.instance)
        service_account_email = instance_resource["serviceAccountEmailAddress"]
        task_instance = context['task_instance']
        task_instance.xcom_push(key="service_account_email", value=service_account_email)


class CloudSqlInstancePatchOperator(CloudSqlBaseOperator):
    """
    Updates settings of a Cloud SQL instance.

    Caution: This is a partial update, so only included values for the settings will be
    updated.

    In the request body, supply the relevant portions of an instance resource, according
    to the rules of patch semantics.
    https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlInstancePatchOperator`

    :param body: Body required by the Cloud SQL patch API, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch#request-body
    :type body: dict
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param project_id: Optional, Google Cloud Platform Project ID.  If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    """
    # [START gcp_sql_patch_template_fields]
    template_fields = ('project_id', 'instance', 'body', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_patch_template_fields]

    @apply_defaults
    def __init__(self,
                 body: dict,
                 instance: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 *args, **kwargs) -> None:
        self.body = body
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def execute(self, context):
        if not self._check_if_instance_exists(self.instance):
            raise AirflowException('Cloud SQL instance with ID {} does not exist. '
                                   'Please specify another instance to patch.'
                                   .format(self.instance))
        else:
            return self._hook.patch_instance(
                project_id=self.project_id,
                body=self.body,
                instance=self.instance)


class CloudSqlInstanceDeleteOperator(CloudSqlBaseOperator):
    """
    Deletes a Cloud SQL instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlInstanceDeleteOperator`

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param project_id: Optional, Google Cloud Platform Project ID. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    """
    # [START gcp_sql_delete_template_fields]
    template_fields = ('project_id', 'instance', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 instance: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 *args, **kwargs) -> None:
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def execute(self, context):
        if not self._check_if_instance_exists(self.instance):
            print("Cloud SQL instance with ID {} does not exist. Aborting delete."
                  .format(self.instance))
            return True
        else:
            return self._hook.delete_instance(
                project_id=self.project_id,
                instance=self.instance)


class CloudSqlInstanceDatabaseCreateOperator(CloudSqlBaseOperator):
    """
    Creates a new database inside a Cloud SQL instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlInstanceDatabaseCreateOperator`

    :param instance: Database instance ID. This does not include the project ID.
    :type instance: str
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert#request-body
    :type body: dict
    :param project_id: Optional, Google Cloud Platform Project ID. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    :param validate_body: Whether the body should be validated. Defaults to True.
    :type validate_body: bool
    """
    # [START gcp_sql_db_create_template_fields]
    template_fields = ('project_id', 'instance', 'body', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_db_create_template_fields]

    @apply_defaults
    def __init__(self,
                 instance: str,
                 body: dict,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 validate_body: bool = True,
                 *args, **kwargs) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self):
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_DATABASE_CREATE_VALIDATION,
                                  api_version=self.api_version).validate(self.body)

    def execute(self, context):
        self._validate_body_fields()
        database = self.body.get("name")
        if not database:
            self.log.error("Body doesn't contain 'name'. Cannot check if the"
                           " database already exists in the instance %s.", self.instance)
            return False
        if self._check_if_db_exists(database):
            self.log.info("Cloud SQL instance with ID %s already contains database"
                          " '%s'. Aborting database insert.", self.instance, database)
            return True
        else:
            return self._hook.create_database(project_id=self.project_id,
                                              instance=self.instance,
                                              body=self.body)


class CloudSqlInstanceDatabasePatchOperator(CloudSqlBaseOperator):
    """
    Updates a resource containing information about a database inside a Cloud SQL
    instance using patch semantics.
    See: https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlInstanceDatabasePatchOperator`

    :param instance: Database instance ID. This does not include the project ID.
    :type instance: str
    :param database: Name of the database to be updated in the instance.
    :type database: str
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/patch#request-body
    :type body: dict
    :param project_id: Optional, Google Cloud Platform Project ID.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    :param validate_body: Whether the body should be validated. Defaults to True.
    :type validate_body: bool
    """
    # [START gcp_sql_db_patch_template_fields]
    template_fields = ('project_id', 'instance', 'body', 'database', 'gcp_conn_id',
                       'api_version')
    # [END gcp_sql_db_patch_template_fields]

    @apply_defaults
    def __init__(self,
                 instance: str,
                 database: str,
                 body: dict,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 validate_body: bool = True,
                 *args, **kwargs) -> None:
        self.database = database
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")
        if not self.database:
            raise AirflowException("The required parameter 'database' is empty")

    def _validate_body_fields(self):
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_DATABASE_PATCH_VALIDATION,
                                  api_version=self.api_version).validate(self.body)

    def execute(self, context):
        self._validate_body_fields()
        if not self._check_if_db_exists(self.database):
            raise AirflowException("Cloud SQL instance with ID {instance} does not contain "
                                   "database '{database}'. "
                                   "Please specify another database to patch.".
                                   format(instance=self.instance, database=self.database))
        else:
            return self._hook.patch_database(
                project_id=self.project_id,
                instance=self.instance,
                database=self.database,
                body=self.body)


class CloudSqlInstanceDatabaseDeleteOperator(CloudSqlBaseOperator):
    """
    Deletes a database from a Cloud SQL instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlInstanceDatabaseDeleteOperator`

    :param instance: Database instance ID. This does not include the project ID.
    :type instance: str
    :param database: Name of the database to be deleted in the instance.
    :type database: str
    :param project_id: Optional, Google Cloud Platform Project ID. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    """
    # [START gcp_sql_db_delete_template_fields]
    template_fields = ('project_id', 'instance', 'database', 'gcp_conn_id',
                       'api_version')
    # [END gcp_sql_db_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 instance: str,
                 database: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 *args, **kwargs) -> None:
        self.database = database
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super()._validate_inputs()
        if not self.database:
            raise AirflowException("The required parameter 'database' is empty")

    def execute(self, context):
        if not self._check_if_db_exists(self.database):
            print("Cloud SQL instance with ID {} does not contain database '{}'. "
                  "Aborting database delete."
                  .format(self.instance, self.database))
            return True
        else:
            return self._hook.delete_database(
                project_id=self.project_id,
                instance=self.instance,
                database=self.database)


class CloudSqlInstanceExportOperator(CloudSqlBaseOperator):
    """
    Exports data from a Cloud SQL instance to a Cloud Storage bucket as a SQL dump
    or CSV file.

    Note: This operator is idempotent. If executed multiple times with the same
    export file URI, the export file in GCS will simply be overridden.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlInstanceImportOperator`

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
    :type body: dict
    :param project_id: Optional, Google Cloud Platform Project ID. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    :param validate_body: Whether the body should be validated. Defaults to True.
    :type validate_body: bool
    """
    # [START gcp_sql_export_template_fields]
    template_fields = ('project_id', 'instance', 'body', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_export_template_fields]

    @apply_defaults
    def __init__(self,
                 instance: str,
                 body: dict,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 validate_body: bool = True,
                 *args, **kwargs) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self):
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_EXPORT_VALIDATION,
                                  api_version=self.api_version).validate(self.body)

    def execute(self, context):
        self._validate_body_fields()
        return self._hook.export_instance(
            project_id=self.project_id,
            instance=self.instance,
            body=self.body)


class CloudSqlInstanceImportOperator(CloudSqlBaseOperator):
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
        :ref:`howto/operator:CloudSqlInstanceImportOperator`

    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param body: The request body, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
    :type body: dict
    :param project_id: Optional, Google Cloud Platform Project ID. If set to None or missing,
            the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1beta4).
    :type api_version: str
    :param validate_body: Whether the body should be validated. Defaults to True.
    :type validate_body: bool
    """
    # [START gcp_sql_import_template_fields]
    template_fields = ('project_id', 'instance', 'body', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_import_template_fields]

    @apply_defaults
    def __init__(self,
                 instance: str,
                 body: dict,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v1beta4',
                 validate_body: bool = True,
                 *args, **kwargs) -> None:
        self.body = body
        self.validate_body = validate_body
        super().__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super()._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self):
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_IMPORT_VALIDATION,
                                  api_version=self.api_version).validate(self.body)

    def execute(self, context):
        self._validate_body_fields()
        return self._hook.import_instance(
            project_id=self.project_id,
            instance=self.instance,
            body=self.body)


class CloudSqlQueryOperator(BaseOperator):
    """
    Performs DML or DDL query on an existing Cloud Sql instance. It optionally uses
    cloud-sql-proxy to establish secure connection with the database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSqlQueryOperator`

    :param sql: SQL query or list of queries to run (should be DML or DDL query -
        this operator does not return any data from the database,
        so it is useless to pass it DQL queries. Note that it is responsibility of the
        author of the queries to make sure that the queries are idempotent. For example
        you can use CREATE TABLE IF NOT EXISTS to create a table.
    :type sql: str or list[str]
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform for
        cloud-sql-proxy authentication.
    :type gcp_conn_id: str
    :param gcp_cloudsql_conn_id: The connection ID used to connect to Google Cloud SQL
       its schema should be gcpcloudsql://.
       See :class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook` for
       details on how to define gcpcloudsql:// connection.
    :type gcp_cloudsql_conn_id: str
    """
    # [START gcp_sql_query_template_fields]
    template_fields = ('sql', 'gcp_cloudsql_conn_id', 'gcp_conn_id')
    template_ext = ('.sql',)
    # [END gcp_sql_query_template_fields]

    @apply_defaults
    def __init__(self,
                 sql: Union[List[str], str],
                 autocommit: bool = False,
                 parameters: Optional[Union[Dict, Iterable]] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 gcp_cloudsql_conn_id: str = 'google_cloud_sql_default',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.gcp_cloudsql_conn_id = gcp_cloudsql_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.gcp_connection = BaseHook.get_connection(self.gcp_conn_id)
        self.cloudsql_db_hook = CloudSqlDatabaseHook(
            gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
            gcp_conn_id=gcp_conn_id,
            default_gcp_project_id=self.gcp_connection.extra_dejson.get(
                'extra__google_cloud_platform__project'))
        self.cloud_sql_proxy_runner = None
        self.database_hook = None

    def _execute_query(self):
        try:
            if self.cloudsql_db_hook.use_proxy:
                self.cloud_sql_proxy_runner = self.cloudsql_db_hook. \
                    get_sqlproxy_runner()
                self.cloudsql_db_hook.free_reserved_port()
                # There is very, very slim chance that the socket will
                # be taken over here by another bind(0).
                # It's quite unlikely to happen though!
                self.cloud_sql_proxy_runner.start_proxy()
            self.log.info('Executing: "%s"', self.sql)
            self.database_hook.run(self.sql, self.autocommit,
                                   parameters=self.parameters)
        finally:
            if self.cloud_sql_proxy_runner:
                self.cloud_sql_proxy_runner.stop_proxy()
                self.cloud_sql_proxy_runner = None

    def execute(self, context):
        self.cloudsql_db_hook.validate_ssl_certs()
        self.cloudsql_db_hook.create_connection()

        try:
            self.cloudsql_db_hook.validate_socket_path_length()
            self.database_hook = self.cloudsql_db_hook.get_database_hook()
            try:
                self._execute_query()
            finally:
                self.cloudsql_db_hook.cleanup_database_hook()
        finally:
            self.cloudsql_db_hook.delete_connection()
            self.cloudsql_db_hook = None
