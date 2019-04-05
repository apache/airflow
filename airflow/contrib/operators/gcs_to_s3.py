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
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class GoogleCloudStorageToS3Operator(GoogleCloudStorageListOperator):
    """
    Synchronizes a Google Cloud Storage bucket with an S3 bucket.

    :param bucket: The Google Cloud Storage bucket to find the objects. (templated)
    :type bucket: str
    :param prefix: Prefix string which filters objects whose name begin with
        this prefix. (templated)
    :type prefix: str
    :param delimiter: The delimiter by which you want to filter the objects. (templated)
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :type delimiter: str
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param dest_aws_conn_id: The destination S3 connection
    :type dest_aws_conn_id: str
    :param dest_s3_key: The base S3 key to be used to store the files. (templated)
    :type dest_s3_key: str
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type dest_verify: bool or str
    """
    template_fields = ('bucket', 'prefix', 'delimiter', 'dest_s3_key')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix=None,
                 delimiter=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 dest_aws_conn_id=None,
                 dest_s3_key=None,
                 dest_verify=None,
                 replace=False,
                 *args,
                 **kwargs):

        super().__init__(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            google_cloud_storage_conn_id=google_cloud_storage_conn_id,
            delegate_to=delegate_to,
            *args,
            **kwargs
        )
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_s3_key = dest_s3_key
        self.dest_verify = dest_verify
        self.replace = replace

    def execute(self, context):
        # use the super to list all files in an Google Cloud Storage bucket
        files = super().execute(context)
        s3_hook = S3Hook(aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify)

        if not self.replace:
            # if we are not replacing -> list all files in the S3 bucket
            # and only keep those files which are present in
            # Google Cloud Storage and not in S3
            bucket_name, _ = S3Hook.parse_s3_url(self.dest_s3_key)
            existing_files = s3_hook.list_keys(bucket_name)
            files = list(set(files) - set(existing_files))

        if files:
            hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to
            )

            for file in files:
                file_bytes = hook.download(self.bucket, file)

                dest_key = self.dest_s3_key + file
                self.log.info("Saving file to %s", dest_key)

                s3_hook.load_bytes(file_bytes,
                                   key=dest_key,
                                   replace=self.replace)

            self.log.info("All done, uploaded %d files to S3", len(files))
        else:
            self.log.info("In sync, no files needed to be uploaded to S3")

        return files
