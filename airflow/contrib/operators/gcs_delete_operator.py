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
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageDeleteOperator(BaseOperator):
    """
    Deletes objects from a Google Cloud Storage bucket, either
    from an explicit list of object names or all objects
    matching a prefix.

    :param bucket_name: The GCS bucket to delete from
    :type bucket_name: str
    :param objects: List of objects to delete. These should be the names
        of objects in the bucket, not including gs://bucket/
    :type objects: List[str]
    :param prefix: Prefix of objects to delete. All objects matching this
        prefix in the bucket will be deleted.
    :param google_cloud_storage_conn_id: The connection ID to use for
        Google Cloud Storage
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('bucket_name', 'prefix', 'objects')

    @apply_defaults
    def __init__(self,
                 bucket_name,
                 objects=None,
                 prefix=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):
        self.bucket_name = bucket_name
        self.objects = objects
        self.prefix = prefix
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        assert objects is not None or prefix is not None

        super(GoogleCloudStorageDeleteOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        if self.objects:
            objects = self.objects
        else:
            objects = hook.list(bucket=self.bucket_name,
                                prefix=self.prefix)

        self.log.info("Deleting %s objects from %s",
                      len(objects), self.bucket_name)
        for object_name in objects:
            hook.delete(bucket=self.bucket_name,
                        object=object_name)
