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
from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.contrib.hooks.gcp_sql_hook import CloudSqlHook
from airflow.contrib.utils.gcp_field_validator import GcpBodyFieldValidator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

SETTINGS = 'settings'
SETTINGS_VERSION = 'settingsVersion'

CLOUD_SQL_VALIDATION = [
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


class CloudSqlBaseOperator(BaseOperator):
    """
    Abstract base operator for Google Cloud SQL operators to inherit from.

    :param project_id: Project ID of the Google Cloud Platform project to operate it.
    :type project_id: str
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    @apply_defaults
    def __init__(self,
                 project_id,
                 instance,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1beta4',
                 *args, **kwargs):
        self.project_id = project_id
        self.instance = instance
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self._hook = CloudSqlHook(gcp_conn_id=self.gcp_conn_id,
                                  api_version=self.api_version)
        super(CloudSqlBaseOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance:
            raise AirflowException("The required parameter 'instance' is empty")

    def _check_if_instance_exists(self, instance):
        try:
            return self._hook.get_instance(self.project_id, instance)
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

    :param project_id: Project ID of the project to which the newly created Cloud SQL
        instances should belong.
    :type project_id: str
    :param body: Body required by the Cloud SQL insert API, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert
        #request-body
    :type body: dict
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :param validate_body: True if body should be validated, False otherwise.
    :type validate_body: bool
    """
    # [START gcp_sql_create_template_fields]
    template_fields = ('project_id', 'instance', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_create_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 body,
                 instance,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1beta4',
                 validate_body=True,
                 *args, **kwargs):
        self.body = body
        self.validate_body = validate_body
        super(CloudSqlInstanceCreateOperator, self).__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super(CloudSqlInstanceCreateOperator, self)._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def _validate_body_fields(self):
        if self.validate_body:
            GcpBodyFieldValidator(CLOUD_SQL_VALIDATION,
                                  api_version=self.api_version).validate(self.body)

    def execute(self, context):
        self._validate_body_fields()
        if not self._check_if_instance_exists(self.instance):
            return self._hook.create_instance(self.project_id, self.body)
        else:
            self.log.info("Cloud SQL instance with ID {} already exists. "
                          "Aborting create.".format(self.instance))
            return True


class CloudSqlInstancePatchOperator(CloudSqlBaseOperator):
    """
    Updates settings of a Cloud SQL instance.

    Caution: This is a partial update, so only included values for the settings will be
    updated.

    In the request body, supply the relevant portions of an instance resource, according
    to the rules of patch semantics.
    https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

    :param project_id: Project ID of the project that contains the instance.
    :type project_id: str
    :param body: Body required by the Cloud SQL patch API, as described in
        https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch#request-body
    :type body: dict
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_sql_patch_template_fields]
    template_fields = ('project_id', 'instance', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_patch_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 body,
                 instance,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1beta4',
                 *args, **kwargs):
        self.body = body
        super(CloudSqlInstancePatchOperator, self).__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def _validate_inputs(self):
        super(CloudSqlInstancePatchOperator, self)._validate_inputs()
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty")

    def execute(self, context):
        if not self._check_if_instance_exists(self.instance):
            raise AirflowException('Cloud SQL instance with ID {} does not exist. '
                                   'Please specify another instance to patch.'
                                   .format(self.instance))
        else:
            return self._hook.patch_instance(self.project_id, self.body, self.instance)


class CloudSqlInstanceDeleteOperator(CloudSqlBaseOperator):
    """
    Deletes a Cloud SQL instance.

    :param project_id: Project ID of the project that contains the instance to be deleted.
    :type project_id: str
    :param instance: Cloud SQL instance ID. This does not include the project ID.
    :type instance: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_sql_delete_template_fields]
    template_fields = ('project_id', 'instance', 'gcp_conn_id', 'api_version')
    # [END gcp_sql_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1beta4',
                 *args, **kwargs):
        super(CloudSqlInstanceDeleteOperator, self).__init__(
            project_id=project_id, instance=instance, gcp_conn_id=gcp_conn_id,
            api_version=api_version, *args, **kwargs)

    def execute(self, context):
        if not self._check_if_instance_exists(self.instance):
            print("Cloud SQL instance with ID {} does not exist. Aborting delete."
                  .format(self.instance))
            return True
        else:
            return self._hook.delete_instance(self.project_id, self.instance)
