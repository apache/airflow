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
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageObjectSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.
    Create a new GoogleCloudStorageObjectSensor.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        :param google_cloud_storage_conn_id: The connection ID to use when
            connecting to Google cloud storage.
        :type google_cloud_storage_conn_id: str
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: str
    """
    template_fields = ('bucket', 'object')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 object,  # pylint:disable=redefined-builtin
                 google_cloud_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):

        super(GoogleCloudStorageObjectSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info('Sensor checks existence of : %s, %s', self.bucket, self.object)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return hook.exists(self.bucket, self.object)


def ts_function(context):
    """
    Default callback for the GoogleCloudStorageObjectUpdatedSensor. The default
    behaviour is check for the object being updated after execution_date +
    schedule_interval.
    """
    return context['execution_date'] + context['dag'].schedule_interval


class GoogleCloudStorageObjectUpdatedSensor(BaseSensorOperator):
    """
    Checks if an object is updated in Google Cloud Storage.
    Create a new GoogleCloudStorageObjectUpdatedSensor.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to download in the Google cloud
            storage bucket.
        :type object: str
        :param ts_func: Callback for defining the update condition. The default callback
            returns execution_date + schedule_interval. The callback takes the context
            as parameter.
        :type ts_func: function
        :param google_cloud_storage_conn_id: The connection ID to use when
            connecting to Google cloud storage.
        :type google_cloud_storage_conn_id: str
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: str
    """
    template_fields = ('bucket', 'object')
    template_ext = ('.sql',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 object,  # pylint:disable=redefined-builtin
                 ts_func=ts_function,
                 google_cloud_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):

        super(GoogleCloudStorageObjectUpdatedSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.ts_func = ts_func
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info('Sensor checks existence of : %s, %s', self.bucket, self.object)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return hook.is_updated_after(self.bucket, self.object, self.ts_func(context))


class GoogleCloudStoragePrefixSensor(BaseSensorOperator):
    """
    Checks for the existence of a files at prefix in Google Cloud Storage bucket.
    Create a new GoogleCloudStorageObjectSensor.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param prefix: The name of the prefix to check in the Google cloud
            storage bucket.
        :type prefix: str
        :param google_cloud_storage_conn_id: The connection ID to use when
            connecting to Google cloud storage.
        :type google_cloud_storage_conn_id: str
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: str
    """
    template_fields = ('bucket', 'prefix')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix,
                 google_cloud_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):
        super(GoogleCloudStoragePrefixSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info('Sensor checks existence of objects: %s, %s',
                      self.bucket, self.prefix)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return bool(hook.list(self.bucket, prefix=self.prefix))
