# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.configuration import conf
from airflow.contrib.pipeline import Pipeline
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import json
import os
from tempfile import NamedTemporaryFile


def json_deserialize_from_filename(filename, context):
    with open(filename, 'r') as f:
        return json.load(f)


class GCSPipeline(Pipeline):

    def __init__(
            self,
            bucket,
            blob_prefix,
            gcs_connection_id,
            downstream_task,
            downstream_key,
            upstream_task,
            upstream_key=None,
            extension='.air',
            serialize=None,
            deserialize=None):
        """
        GCSPipeline serializes data to Google Cloud Storage.

        Files will be written to gs://{bucket}/{blob_prefix}/{unique_key}

        serialize() receives data and is expected to return a representation
        that can be written to GCS.

        deserialize() receives a temporary filename that contains the GCS data.
        """

        if deserialize is None:
            deserialize = json_deserialize_from_filename

        super(GCSPipeline, self).__init__(
            downstream_key=downstream_key,
            upstream_task=upstream_task,
            downstream_task=downstream_task,
            upstream_key=upstream_key,
            serialize=serialize,
            deserialize=deserialize)

        self.extension = extension

        bucket = bucket or conf.get('pipeline', 'gcloud_storage_bucket')
        if not bucket:
            raise ValueError('No GCS bucket provided and no default set.')
        self.bucket = bucket

        blob_prefix = blob_prefix or conf.get(
            'pipeline', 'gcloud_storage_prefix')
        if not blob_prefix:
            raise ValueError('No GCS blob prefix provided and no default set.')
        self.blob_prefix = blob_prefix

        self.gcs_connection_id = gcs_connection_id or conf.get(
            'pipeline', 'gcloud_storage_connection_id')

        self.gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=gcs_connection_id)

    def _serialize(self, data, context):
        gcs_path = os.path.join(
            self.blob_prefix.strip('/'),
            self.generate_unique_id(context, extension=self.extension))
        with NamedTemporaryFile() as tmp:
            serialized_data = self.serialize(data, context)
            if not isinstance(serialized_data, bytes):
                serialized_data = serialized_data.encode()
            tmp.write(serialized_data)
            tmp.seek(0)
            self.gcs_hook.upload(self.bucket, gcs_path, tmp.name)
        return gcs_path

    def _deserialize(self, gcs_path, context):
        with NamedTemporaryFile() as tmp:
            self.gcs_hook.download(self.bucket, gcs_path, tmp.name)
            tmp.seek(0)
            data = self.deserialize(tmp.name, context)
        return data
