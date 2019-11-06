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
from airflow.contrib.hooks.datastore_hook import DatastoreHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DatastoreExportOperator(BaseOperator):
    """
    Export entities from Google Cloud Datastore to Cloud Storage

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
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
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
    """

    @apply_defaults
    def __init__(self,
                 bucket,
                 namespace=None,
                 datastore_conn_id='google_cloud_default',
                 cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 entity_filter=None,
                 labels=None,
                 polling_interval_in_seconds=10,
                 overwrite_existing=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.datastore_conn_id = datastore_conn_id
        self.cloud_storage_conn_id = cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.bucket = bucket
        self.namespace = namespace
        self.entity_filter = entity_filter
        self.labels = labels
        self.polling_interval_in_seconds = polling_interval_in_seconds
        self.overwrite_existing = overwrite_existing
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context):
        self.log.info('Exporting data to Cloud Storage bucket ' + self.bucket)

        if self.overwrite_existing and self.namespace:
            gcs_hook = GoogleCloudStorageHook(self.cloud_storage_conn_id)
            objects = gcs_hook.list(self.bucket, prefix=self.namespace)
            for o in objects:
                gcs_hook.delete(self.bucket, o)

        ds_hook = DatastoreHook(self.datastore_conn_id, self.delegate_to)
        result = ds_hook.export_to_storage_bucket(bucket=self.bucket,
                                                  namespace=self.namespace,
                                                  entity_filter=self.entity_filter,
                                                  labels=self.labels)
        operation_name = result['name']
        result = ds_hook.poll_operation_until_done(operation_name,
                                                   self.polling_interval_in_seconds)

        state = result['metadata']['common']['state']
        if state != 'SUCCESSFUL':
            raise AirflowException('Operation failed: result={}'.format(result))

        return result
