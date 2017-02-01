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

from airflow.dataflow import Dataflow
from airflow import configuration as conf
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import os
from tempfile import NamedTemporaryFile

try:
    import cPickle as pickle
except:
    import pickle


class GCSFileDataflow(Dataflow):
    def __init__(
            self,
            upstream_task,
            index=None,
            bucket=None,
            path_prefix=None,
            gcs_connection_id=None):
        """

        GCSFileDataflow expects the upstream task to return a path to a file on
        the local filesystem. The contents of that file are uploaded to GCS.
        Prior to running the downstream task, the file contents are downloaded
        to a temporary file. The temporary file's path is passed to the
        downstream task. The temporary file is deleted after the downstream
        task runs.

        :param upstream_task: The Operator that produces the Dataflow result
        :type upstream_task: Operator
        :param index: If provided, the source operator's result is indexed
            by this value prior to being passed to the dataflow. For example,
            if the source operator returns a dictionary, the index could be
            used to select a specific key rather than the entire result.
        :type index: object
        :param bucket: The GCS bucket that will be used for storage. If None,
            the dataflow.gcloud_storage_bucket configuration parameter is
            checked.
        :type bucket: string
        :param path_prefix: The path prefix is prepended to the dataflow key
            to specify the storage URI. If None is provided, the
            dataflow.gcloud_storage_prefix configuration value is checked.
        :type path_prefix: string
        :param gcs_connection_id: The connection ID that should be used
            to connect to GCS. If None is provided, the
            dataflow.gcloud_storage_connection_id configuration value is
            checked.
        :type gcs_connection_id: string
        """

        bucket = bucket or conf.get('dataflow', 'gcloud_storage_bucket')
        if not bucket:
            raise ValueError('No GCS bucket provided and no default set.')
        self.bucket = bucket

        path_prefix = path_prefix or conf.get(
            'dataflow', 'gcloud_storage_prefix')
        if not path_prefix:
            raise ValueError('No GCS path prefix provided and no default set.')
        self.path_prefix = path_prefix

        gcs_connection_id = gcs_connection_id or conf.get(
            'dataflow', 'gcloud_storage_connection_id')

        self.gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=gcs_connection_id)

        super(GCSFileDataflow, self).__init__(
            upstream_task=upstream_task,
            index=index)

    def serialize(self, filename, context):
        gcs_path = os.path.join(self.path_prefix.strip('/'), self.key(context))
        self.gcs_hook.upload(
            self.bucket,
            gcs_path,
            filename)
        return gcs_path

    def deserialize(self, gcs_path, context):
        self._tmp_file = NamedTemporaryFile(delete=False)
        self.gcs_hook.download(
            self.bucket,
            gcs_path,
            filename=self._tmp_file.name)
        return self._tmp_file.name

    def clean_up(self, context):
        os.remove(self._tmp_file.name)
        del self._tmp_file


class GCSDataflow(GCSFileDataflow):
    def __init__(
            self,
            upstream_task,
            index=None,
            bucket=None,
            path_prefix=None,
            gcs_connection_id=None,
            serialize=pickle.dump,
            deserialize=pickle.load):
        """

        GCSDataflow uses Google Cloud Storage to store data. It expects the
        upstream task to return an object that is serialized and stored in GCS.
        The object is downloaded and deserialized before being provided to the
        downstream task.

        Note the difference to GCSFileDataflow, which works by transferring
        file paths rather than objects.

        :param upstream_task: The Operator that produces the Dataflow result
        :type upstream_task: Operator
        :param index: If provided, the source operator's result is indexed
            by this value prior to being passed to the dataflow. For example,
            if the source operator returns a dictionary, the index could be
            used to select a specific key rather than the entire result.
        :type index: object
        :param bucket: The GCS bucket that will be used for storage. If None,
            the dataflow.gcloud_storage_bucket configuration parameter is
            checked.
        :type bucket: string
        :param path_prefix: The path prefix is prepended to the dataflow key
            to specify the storage URI. If None is provided, the
            dataflow.gcloud_storage_prefix configuration value is checked.
        :type path_prefix: string
        :param gcs_connection_id: The connection ID that should be used
            to connect to GCS. If None is provided, the
            dataflow.gcloud_storage_connection_id configuration value is
            checked.
        :type gcs_connection_id: string
        :param serialize: A function serialize(data, filepath) that writes data
            to a filepath on the local filesystem.
        :type serialize: callable
        :param deserialize: A function deserialize(filepath) that loads data
            from a filepath on the local filesystem. deserialize() should
            usually be the inverse of serialize(), such that:
                ( data == deserialize(serialize(data, path)) ) is True
        :type deserialize: callable
        """
        self._serialize = serialize
        self._deserialize = deserialize
        super(GCSDataflow, self).__init__(
            upstream_task=upstream_task,
            index=index,
            bucket=bucket,
            path_prefix=path_prefix,
            gcs_connection_id=gcs_connection_id)

    def serialize(self, data, context):

        with NamedTemporaryFile() as tmp:
            self._serialize(data, tmp)
            tmp.seek(0)
            return super(GCSDataflow, self).serialize(tmp.name, context)

    def deserialize(self, data, context):
        fpath = super(GCSDataflow, self).deserialize(data, context)

        with open(fpath, 'rb') as f:
            return self._deserialize(f)
