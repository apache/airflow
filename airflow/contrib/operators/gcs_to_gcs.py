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


class GoogleCloudStorageToGoogleCloudStorageOperator(BaseOperator):
    """
    Copies objects from a bucket to another, with renaming if requested.

    :param source_bucket: The source Google cloud storage bucket where the
         object is. (templated)
    :type source_bucket: str
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket. (templated)
        You can use only one wildcard for objects (filenames) within your
        bucket. The wildcard can appear inside the object name or at the
        end of the object name. Appending a wildcard to the bucket name is
        unsupported.
    :type source_object: str
    :param destination_bucket: The destination Google cloud storage bucket
        where the object should be. (templated)
    :type destination_bucket: str
    :param destination_object: The destination name of the object in the
        destination Google cloud storage bucket. (templated)
        If a wildcard is supplied in the source_object argument, this is the
        prefix that will be prepended to the final destination objects' paths.
        Note that the source path's part before the wildcard will be removed;
        if it needs to be retained it should be appended to destination_object.
        For example, with prefix ``foo/*`` and destination_object ``blah/``, the
        file ``foo/baz`` will be copied to ``blah/baz``; to retain the prefix write
        the destination_object as e.g. ``blah/foo``, in which case the copied file
        will be named ``blah/foo/baz``.
    :type destination_object: str
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :type move_object: bool
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param last_modified_time: When specified, if the object(s) were
        modified after last_modified_time, they will be copied/moved.
        If tzinfo has not been set, UTC will be assumed.
    :type last_modified_time: datetime.datetime

    :Example:

    The following Operator would copy a single file named
    ``sales/sales-2017/january.avro`` in the ``data`` bucket to the file named
    ``copied_sales/2017/january-backup.avro`` in the ``data_backup`` bucket ::

        copy_single_file = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id='copy_single_file',
            source_bucket='data',
            source_object='sales/sales-2017/january.avro',
            destination_bucket='data_backup',
            destination_object='copied_sales/2017/january-backup.avro',
            google_cloud_storage_conn_id=google_cloud_conn_id
        )

    The following Operator would copy all the Avro files from ``sales/sales-2017``
    folder (i.e. with names starting with that prefix) in ``data`` bucket to the
    ``copied_sales/2017`` folder in the ``data_backup`` bucket. ::

        copy_files = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id='copy_files',
            source_bucket='data',
            source_object='sales/sales-2017/*.avro',
            destination_bucket='data_backup',
            destination_object='copied_sales/2017/',
            google_cloud_storage_conn_id=google_cloud_conn_id
        )

    The following Operator would move all the Avro files from ``sales/sales-2017``
    folder (i.e. with names starting with that prefix) in ``data`` bucket to the
    same folder in the ``data_backup`` bucket, deleting the original files in the
    process. ::

        move_files = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id='move_files',
            source_bucket='data',
            source_object='sales/sales-2017/*.avro',
            destination_bucket='data_backup',
            move_object=True,
            google_cloud_storage_conn_id=google_cloud_conn_id
        )

    """
    template_fields = ('source_bucket', 'source_object', 'destination_bucket',
                       'destination_object',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 source_bucket,
                 source_object,
                 destination_bucket=None,
                 destination_object=None,
                 move_object=False,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 last_modified_time=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageToGoogleCloudStorageOperator,
              self).__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.move_object = move_object
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.last_modified_time = last_modified_time
        self.wildcard = '*'

    def execute(self, context):

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        log_message = 'Executing copy of gs://{0}/{1} to gs://{2}/{3}'

        if self.wildcard in self.source_object:
            prefix, delimiter = self.source_object.split(self.wildcard, 1)
            objects = hook.list(self.source_bucket, prefix=prefix, delimiter=delimiter)

            for source_object in objects:
                if self.last_modified_time is not None:
                    # Check to see if object was modified after last_modified_time
                    if hook.is_updated_after(self.source_bucket, source_object,
                                             self.last_modified_time):
                        pass
                    else:
                        continue
                if self.destination_object is None:
                    destination_object = source_object
                else:
                    destination_object = source_object.replace(prefix,
                                                               self.destination_object, 1)
                self.log.info(
                    log_message.format(self.source_bucket, source_object,
                                       self.destination_bucket, destination_object)
                )

                hook.rewrite(self.source_bucket, source_object,
                             self.destination_bucket, destination_object)
                if self.move_object:
                    hook.delete(self.source_bucket, source_object)

        else:
            if self.last_modified_time is not None:
                if hook.is_updated_after(self.source_bucket,
                                         self.source_object,
                                         self.last_modified_time):
                    pass
                else:
                    return

            self.log.info(
                log_message.format(self.source_bucket, self.source_object,
                                   self.destination_bucket or self.source_bucket,
                                   self.destination_object or self.source_object)
            )
            hook.rewrite(self.source_bucket, self.source_object,
                         self.destination_bucket, self.destination_object)

            if self.move_object:
                hook.delete(self.source_bucket, self.source_object)
