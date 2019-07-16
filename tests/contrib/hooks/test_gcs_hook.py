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
import io
import os
import tempfile
import unittest

from google.cloud import storage
from google.cloud import exceptions

from airflow.contrib.hooks import gcs_hook
from airflow.exceptions import AirflowException
from tests.compat import mock
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
GCS_STRING = 'airflow.contrib.hooks.gcs_hook.{}'

EMPTY_CONTENT = b''
PROJECT_ID_TEST = 'project-id'


class TestGCSHookHelperFunctions(unittest.TestCase):
    def test_parse_gcs_url(self):
        """
        Test GCS url parsing
        """

        self.assertEqual(
            gcs_hook._parse_gcs_url('gs://bucket/path/to/blob'),
            ('bucket', 'path/to/blob'))

        # invalid URI
        self.assertRaises(AirflowException, gcs_hook._parse_gcs_url,
                          'gs:/bucket/path/to/blob')

        # trailing slash
        self.assertEqual(
            gcs_hook._parse_gcs_url('gs://bucket/path/to/blob/'),
            ('bucket', 'path/to/blob/'))

        # bucket only
        self.assertEqual(
            gcs_hook._parse_gcs_url('gs://bucket/'), ('bucket', ''))


class TestGoogleCloudStorageHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            GCS_STRING.format('GoogleCloudBaseHook.__init__'),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gcs_hook = gcs_hook.GoogleCloudStorageHook(
                google_cloud_storage_conn_id='test')

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_exists(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        get_bucket_mock = mock_service.return_value.get_bucket
        blob_object = get_bucket_mock.return_value.blob
        exists_method = blob_object.return_value.exists
        exists_method.return_value = True

        # When
        response = self.gcs_hook.exists(bucket_name=test_bucket, object_name=test_object)

        # Then
        self.assertTrue(response)
        get_bucket_mock.assert_called_once_with(test_bucket)
        blob_object.assert_called_once_with(blob_name=test_object)
        exists_method.assert_called_once_with()

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_exists_nonexisting_object(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        get_bucket_mock = mock_service.return_value.get_bucket
        blob_object = get_bucket_mock.return_value.blob
        exists_method = blob_object.return_value.exists
        exists_method.return_value = False

        # When
        response = self.gcs_hook.exists(bucket_name=test_bucket, object_name=test_object)

        # Then
        self.assertFalse(response)

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_copy(self, mock_service, mock_bucket):
        source_bucket = 'test-source-bucket'
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        destination_bucket_instance = mock_bucket
        source_blob = mock_bucket.blob(source_object)
        destination_blob = storage.Blob(
            bucket=destination_bucket_instance,
            name=destination_object)

        # Given
        get_bucket_mock = mock_service.return_value.get_bucket
        get_bucket_mock.return_value = mock_bucket
        copy_method = get_bucket_mock.return_value.copy_blob
        copy_method.return_value = destination_blob

        # When
        response = self.gcs_hook.copy(  # pylint:disable=assignment-from-no-return
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object
        )

        # Then
        self.assertEqual(response, None)
        copy_method.assert_called_once_with(
            blob=source_blob,
            destination_bucket=destination_bucket_instance,
            new_name=destination_object
        )

    def test_copy_fail_same_source_and_destination(self):
        source_bucket = 'test-source-bucket'
        source_object = 'test-source-object'
        destination_bucket = 'test-source-bucket'
        destination_object = 'test-source-object'

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.copy(source_bucket=source_bucket,
                               source_object=source_object,
                               destination_bucket=destination_bucket,
                               destination_object=destination_object)

        self.assertEqual(
            str(e.exception),
            'Either source/destination bucket or source/destination object '
            'must be different, not both the same: bucket=%s, object=%s' %
            (source_bucket, source_object)
        )

    def test_copy_empty_source_bucket(self):
        source_bucket = None
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.copy(source_bucket=source_bucket,
                               source_object=source_object,
                               destination_bucket=destination_bucket,
                               destination_object=destination_object)

        self.assertEqual(
            str(e.exception),
            'source_bucket and source_object cannot be empty.'
        )

    def test_copy_empty_source_object(self):
        source_bucket = 'test-source-object'
        source_object = None
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.copy(source_bucket=source_bucket,
                               source_object=source_object,
                               destination_bucket=destination_bucket,
                               destination_object=destination_object)

        self.assertEqual(
            str(e.exception),
            'source_bucket and source_object cannot be empty.'
        )

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_rewrite(self, mock_service, mock_bucket):
        source_bucket = 'test-source-bucket'
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        source_blob = mock_bucket.blob(source_object)

        # Given
        get_bucket_mock = mock_service.return_value.get_bucket
        get_bucket_mock.return_value = mock_bucket
        get_blob_method = get_bucket_mock.return_value.blob
        rewrite_method = get_blob_method.return_value.rewrite
        rewrite_method.side_effect = [(None, mock.ANY, mock.ANY), (mock.ANY, mock.ANY, mock.ANY)]

        # When
        response = self.gcs_hook.rewrite(  # pylint:disable=assignment-from-no-return
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object)

        # Then
        self.assertEqual(response, None)
        rewrite_method.assert_called_once_with(
            source=source_blob)

    def test_rewrite_empty_source_bucket(self):
        source_bucket = None
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.rewrite(source_bucket=source_bucket,
                                  source_object=source_object,
                                  destination_bucket=destination_bucket,
                                  destination_object=destination_object)

        self.assertEqual(
            str(e.exception),
            'source_bucket and source_object cannot be empty.'
        )

    def test_rewrite_empty_source_object(self):
        source_bucket = 'test-source-object'
        source_object = None
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.rewrite(source_bucket=source_bucket,
                                  source_object=source_object,
                                  destination_bucket=destination_bucket,
                                  destination_object=destination_object)

        self.assertEqual(
            str(e.exception),
            'source_bucket and source_object cannot be empty.'
        )

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_delete(self, mock_service, mock_bucket):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        blob_to_be_deleted = storage.Blob(name=test_object, bucket=mock_bucket)

        get_bucket_method = mock_service.return_value.get_bucket
        get_blob_method = get_bucket_method.return_value.get_blob
        delete_method = get_blob_method.return_value.delete
        delete_method.return_value = blob_to_be_deleted

        response = self.gcs_hook.delete(  # pylint:disable=assignment-from-no-return
            bucket_name=test_bucket,
            object_name=test_object)
        self.assertIsNone(response)

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_delete_nonexisting_object(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        get_bucket_method = mock_service.return_value.get_bucket
        blob = get_bucket_method.return_value.blob
        delete_method = blob.return_value.delete
        delete_method.side_effect = exceptions.NotFound(message="Not Found")

        with self.assertRaises(exceptions.NotFound):
            self.gcs_hook.delete(bucket_name=test_bucket, object_name=test_object)

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_object_get_size(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        returned_file_size = 1200

        get_bucket_method = mock_service.return_value.get_bucket
        get_blob_method = get_bucket_method.return_value.get_blob
        get_blob_method.return_value.size = returned_file_size

        response = self.gcs_hook.get_size(bucket_name=test_bucket,
                                          object_name=test_object)

        self.assertEqual(response, returned_file_size)
        get_blob_method.return_value.reload.assert_called_once_with()

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_object_get_crc32c(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        returned_file_crc32c = "xgdNfQ=="

        get_bucket_method = mock_service.return_value.get_bucket
        get_blob_method = get_bucket_method.return_value.get_blob
        get_blob_method.return_value.crc32c = returned_file_crc32c

        response = self.gcs_hook.get_crc32c(bucket_name=test_bucket,
                                            object_name=test_object)

        self.assertEqual(response, returned_file_crc32c)

        # Check that reload method is called
        get_blob_method.return_value.reload.assert_called_once_with()

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_object_get_md5hash(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        returned_file_md5hash = "leYUJBUWrRtks1UeUFONJQ=="

        get_bucket_method = mock_service.return_value.get_bucket
        get_blob_method = get_bucket_method.return_value.get_blob
        get_blob_method.return_value.md5_hash = returned_file_md5hash

        response = self.gcs_hook.get_md5hash(bucket_name=test_bucket,
                                             object_name=test_object)

        self.assertEqual(response, returned_file_md5hash)

        # Check that reload method is called
        get_blob_method.return_value.reload.assert_called_once_with()

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_create_bucket(self, mock_service, mock_bucket):
        test_bucket = 'test_bucket'
        test_project = 'test-project'
        test_location = 'EU'
        test_labels = {'env': 'prod'}
        test_storage_class = 'MULTI_REGIONAL'

        mock_service.return_value.bucket.return_value.create.return_value = None
        mock_bucket.return_value.storage_class = test_storage_class
        mock_bucket.return_value.labels = test_labels

        sample_bucket = mock_service().bucket(bucket_name=test_bucket)

        response = self.gcs_hook.create_bucket(
            bucket_name=test_bucket,
            storage_class=test_storage_class,
            location=test_location,
            labels=test_labels,
            project_id=test_project
        )

        self.assertEqual(response, sample_bucket.id)

        self.assertEqual(sample_bucket.storage_class, test_storage_class)
        self.assertEqual(sample_bucket.labels, test_labels)

        mock_service.return_value.bucket.return_value.create.assert_called_with(
            project=test_project, location=test_location
        )

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_create_bucket_with_resource(self, mock_service, mock_bucket):
        test_bucket = 'test_bucket'
        test_project = 'test-project'
        test_location = 'EU'
        test_labels = {'env': 'prod'}
        test_storage_class = 'MULTI_REGIONAL'
        test_versioning_enabled = {"enabled": True}

        mock_service.return_value.bucket.return_value.create.return_value = None
        mock_bucket.return_value.storage_class = test_storage_class
        mock_bucket.return_value.labels = test_labels
        mock_bucket.return_value.versioning_enabled = True

        sample_bucket = mock_service().bucket(bucket_name=test_bucket)

        # sample_bucket = storage.Bucket(client=mock_service, name=test_bucket)
        # Assert for resource other than None.
        response = self.gcs_hook.create_bucket(
            bucket_name=test_bucket,
            resource={"versioning": test_versioning_enabled},
            storage_class=test_storage_class,
            location=test_location,
            labels=test_labels,
            project_id=test_project
        )
        self.assertEqual(response, sample_bucket.id)

        mock_service.return_value.bucket.return_value._patch_property.assert_called_with(
            name='versioning', value=test_versioning_enabled
        )

        mock_service.return_value.bucket.return_value.create.assert_called_with(
            project=test_project, location=test_location
        )

    @mock.patch('google.cloud.storage.Bucket.blob')
    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_compose(self, mock_service, mock_blob):
        test_bucket = 'test_bucket'
        test_source_objects = ['test_object_1', 'test_object_2', 'test_object_3']
        test_destination_object = 'test_object_composed'

        mock_service.return_value.get_bucket.return_value\
            .blob.return_value = mock_blob(blob_name=mock.ANY)
        method = mock_service.return_value.get_bucket.return_value.blob\
            .return_value.compose

        self.gcs_hook.compose(
            bucket_name=test_bucket,
            source_objects=test_source_objects,
            destination_object=test_destination_object
        )

        method.assert_called_once_with(
            sources=[
                mock_blob(blob_name=source_object) for source_object in test_source_objects
            ])

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_compose_with_empty_source_objects(self, mock_service):  # pylint:disable=unused-argument
        test_bucket = 'test_bucket'
        test_source_objects = []
        test_destination_object = 'test_object_composed'

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object
            )

        self.assertEqual(
            str(e.exception),
            'source_objects cannot be empty.'
        )

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_compose_without_bucket(self, mock_service):  # pylint:disable=unused-argument
        test_bucket = None
        test_source_objects = ['test_object_1', 'test_object_2', 'test_object_3']
        test_destination_object = 'test_object_composed'

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object
            )

        self.assertEqual(
            str(e.exception),
            'bucket_name and destination_object cannot be empty.'
        )

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_compose_without_destination_object(self, mock_service):  # pylint:disable=unused-argument
        test_bucket = 'test_bucket'
        test_source_objects = ['test_object_1', 'test_object_2', 'test_object_3']
        test_destination_object = None

        with self.assertRaises(ValueError) as e:
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object
            )

        self.assertEqual(
            str(e.exception),
            'bucket_name and destination_object cannot be empty.'
        )

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_download_as_string(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        test_object_bytes = io.BytesIO(b"input")

        download_method = mock_service.return_value.get_bucket.return_value \
            .blob.return_value.download_as_string
        download_method.return_value = test_object_bytes

        response = self.gcs_hook.download(bucket_name=test_bucket,
                                          object_name=test_object,
                                          filename=None)

        self.assertEqual(response, test_object_bytes)
        download_method.assert_called_once_with()

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_download_to_file(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        test_object_bytes = io.BytesIO(b"input")
        test_file = 'test_file'

        download_filename_method = mock_service.return_value.get_bucket.return_value \
            .blob.return_value.download_to_filename
        download_filename_method.return_value = None

        download_as_a_string_method = mock_service.return_value.get_bucket.return_value \
            .blob.return_value.download_as_string
        download_as_a_string_method.return_value = test_object_bytes

        response = self.gcs_hook.download(bucket_name=test_bucket,
                                          object_name=test_object,
                                          filename=test_file)

        self.assertEqual(response, test_object_bytes)
        download_filename_method.assert_called_once_with(test_file)


class TestGoogleCloudStorageHookUpload(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__')):
            self.gcs_hook = gcs_hook.GoogleCloudStorageHook(
                google_cloud_storage_conn_id='test'
            )

        # generate a 384KiB test file (larger than the minimum 256KiB multipart chunk size)
        self.testfile = tempfile.NamedTemporaryFile(delete=False)
        self.testfile.write(b"x" * 393216)
        self.testfile.flush()

    def tearDown(self):
        os.unlink(self.testfile.name)

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_upload(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        upload_method = mock_service.return_value.get_bucket.return_value\
            .blob.return_value.upload_from_filename
        upload_method.return_value = None

        response = self.gcs_hook.upload(test_bucket,  # pylint:disable=assignment-from-no-return
                                        test_object,
                                        self.testfile.name)

        self.assertIsNone(response)
        upload_method.assert_called_once_with(
            filename=self.testfile.name,
            content_type='application/octet-stream'
        )

    @mock.patch(GCS_STRING.format('GoogleCloudStorageHook.get_conn'))
    def test_upload_gzip(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        upload_method = mock_service.return_value.get_bucket.return_value \
            .blob.return_value.upload_from_filename
        upload_method.return_value = None

        response = self.gcs_hook.upload(test_bucket,  # pylint:disable=assignment-from-no-return
                                        test_object,
                                        self.testfile.name,
                                        gzip=True)
        self.assertFalse(os.path.exists(self.testfile.name + '.gz'))
        self.assertIsNone(response)
