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

import unittest
from unittest import mock

from google.api_core.exceptions import AlreadyExists
from google.cloud.vision_v1.types import Product, ProductSet, ReferenceImage

from airflow.providers.google.cloud.operators.vision import (
    CloudVisionAddProductToProductSetOperator,
    CloudVisionCreateProductOperator,
    CloudVisionCreateProductSetOperator,
    CloudVisionCreateReferenceImageOperator,
    CloudVisionDeleteProductOperator,
    CloudVisionDeleteProductSetOperator,
    CloudVisionDeleteReferenceImageOperator,
    CloudVisionDetectImageLabelsOperator,
    CloudVisionDetectImageSafeSearchOperator,
    CloudVisionDetectTextOperator,
    CloudVisionGetProductOperator,
    CloudVisionGetProductSetOperator,
    CloudVisionImageAnnotateOperator,
    CloudVisionRemoveProductFromProductSetOperator,
    CloudVisionTextDetectOperator,
    CloudVisionUpdateProductOperator,
    CloudVisionUpdateProductSetOperator,
)

PRODUCTSET_TEST = ProductSet(display_name='Test Product Set')
PRODUCTSET_ID_TEST = 'my-productset'
PRODUCT_TEST = Product(display_name='My Product 1', product_category='toys')
PRODUCT_ID_TEST = 'my-product'
REFERENCE_IMAGE_TEST = ReferenceImage(uri='gs://bucket_name/file.txt')
REFERENCE_IMAGE_ID_TEST = 'my-reference-image'
ANNOTATE_REQUEST_TEST = {'image': {'source': {'image_uri': 'https://foo.com/image.jpg'}}}
ANNOTATE_REQUEST_BATCH_TEST = [
    {'image': {'source': {'image_uri': 'https://foo.com/image1.jpg'}}},
    {'image': {'source': {'image_uri': 'https://foo.com/image2.jpg'}}},
]
LOCATION_TEST = 'europe-west1'
GCP_CONN_ID = 'google_cloud_default'
DETECT_TEST_IMAGE = {"source": {"image_uri": "test_uri"}}


class TestCloudVisionProductSetCreate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.create_product_set.return_value = {}
        op = CloudVisionCreateProductSetOperator(
            location=LOCATION_TEST, product_set=PRODUCTSET_TEST, task_id='id'
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_product_set.assert_called_once_with(
            location=LOCATION_TEST,
            product_set=PRODUCTSET_TEST,
            product_set_id=None,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_already_exists(self, mock_hook):
        mock_hook.return_value.create_product_set.side_effect = AlreadyExists(message='')
        # Exception AlreadyExists not raised, caught in the operator's execute() - idempotence
        op = CloudVisionCreateProductSetOperator(
            location=LOCATION_TEST,
            product_set=PRODUCTSET_TEST,
            product_set_id=PRODUCTSET_ID_TEST,
            project_id='mock-project-id',
            task_id='id',
        )
        result = op.execute(None)
        assert PRODUCTSET_ID_TEST == result


class TestCloudVisionProductSetUpdate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.update_product_set.return_value = {}
        op = CloudVisionUpdateProductSetOperator(
            location=LOCATION_TEST, product_set=PRODUCTSET_TEST, task_id='id'
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_product_set.assert_called_once_with(
            location=LOCATION_TEST,
            product_set=PRODUCTSET_TEST,
            product_set_id=None,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
            update_mask=None,
        )


class TestCloudVisionProductSetGet(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.get_product_set.return_value = {}
        op = CloudVisionGetProductSetOperator(
            location=LOCATION_TEST, product_set_id=PRODUCTSET_ID_TEST, task_id='id'
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_product_set.assert_called_once_with(
            location=LOCATION_TEST,
            product_set_id=PRODUCTSET_ID_TEST,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionProductSetDelete(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.delete_product_set.return_value = {}
        op = CloudVisionDeleteProductSetOperator(
            location=LOCATION_TEST, product_set_id=PRODUCTSET_ID_TEST, task_id='id'
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_product_set.assert_called_once_with(
            location=LOCATION_TEST,
            product_set_id=PRODUCTSET_ID_TEST,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionProductCreate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.create_product.return_value = {}
        op = CloudVisionCreateProductOperator(location=LOCATION_TEST, product=PRODUCT_TEST, task_id='id')
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_product.assert_called_once_with(
            location=LOCATION_TEST,
            product=PRODUCT_TEST,
            product_id=None,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_already_exists(self, mock_hook):
        mock_hook.return_value.create_product.side_effect = AlreadyExists(message='')
        # Exception AlreadyExists not raised, caught in the operator's execute() - idempotence
        op = CloudVisionCreateProductOperator(
            location=LOCATION_TEST,
            product=PRODUCT_TEST,
            product_id=PRODUCT_ID_TEST,
            project_id='mock-project-id',
            task_id='id',
        )
        result = op.execute(None)
        assert PRODUCT_ID_TEST == result


class TestCloudVisionProductGet(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.get_product.return_value = {}
        op = CloudVisionGetProductOperator(location=LOCATION_TEST, product_id=PRODUCT_ID_TEST, task_id='id')
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_product.assert_called_once_with(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionProductUpdate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.update_product.return_value = {}
        op = CloudVisionUpdateProductOperator(location=LOCATION_TEST, product=PRODUCT_TEST, task_id='id')
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_product.assert_called_once_with(
            location=LOCATION_TEST,
            product=PRODUCT_TEST,
            product_id=None,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
            update_mask=None,
        )


class TestCloudVisionProductDelete(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.delete_product.return_value = {}
        op = CloudVisionDeleteProductOperator(
            location=LOCATION_TEST, product_id=PRODUCT_ID_TEST, task_id='id'
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_product.assert_called_once_with(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionReferenceImageCreate(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.vision.CloudVisionHook',
    )
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.create_reference_image.return_value = {}
        op = CloudVisionCreateReferenceImageOperator(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            reference_image=REFERENCE_IMAGE_TEST,
            task_id='id',
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_reference_image.assert_called_once_with(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            reference_image=REFERENCE_IMAGE_TEST,
            reference_image_id=None,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(
        'airflow.providers.google.cloud.operators.vision.CloudVisionHook',
        **{'return_value.create_reference_image.side_effect': AlreadyExists("MESSAGe")},
    )
    def test_already_exists(self, mock_hook):
        # Exception AlreadyExists not raised, caught in the operator's execute() - idempotence
        op = CloudVisionCreateReferenceImageOperator(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            reference_image=REFERENCE_IMAGE_TEST,
            task_id='id',
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_reference_image.assert_called_once_with(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            reference_image=REFERENCE_IMAGE_TEST,
            reference_image_id=None,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionReferenceImageDelete(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.vision.CloudVisionHook',
    )
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.delete_reference_image.return_value = {}
        op = CloudVisionDeleteReferenceImageOperator(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            reference_image_id=REFERENCE_IMAGE_ID_TEST,
            task_id='id',
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_reference_image.assert_called_once_with(
            location=LOCATION_TEST,
            product_id=PRODUCT_ID_TEST,
            reference_image_id=REFERENCE_IMAGE_ID_TEST,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionAddProductToProductSetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        op = CloudVisionAddProductToProductSetOperator(
            location=LOCATION_TEST,
            product_set_id=PRODUCTSET_ID_TEST,
            product_id=PRODUCT_ID_TEST,
            task_id='id',
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.add_product_to_product_set.assert_called_once_with(
            product_set_id=PRODUCTSET_ID_TEST,
            product_id=PRODUCT_ID_TEST,
            location=LOCATION_TEST,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionRemoveProductFromProductSetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path(self, mock_hook):
        op = CloudVisionRemoveProductFromProductSetOperator(
            location=LOCATION_TEST,
            product_set_id=PRODUCTSET_ID_TEST,
            product_id=PRODUCT_ID_TEST,
            task_id='id',
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.remove_product_from_product_set.assert_called_once_with(
            product_set_id=PRODUCTSET_ID_TEST,
            product_id=PRODUCT_ID_TEST,
            location=LOCATION_TEST,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudVisionAnnotateImageOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path_for_one_image(self, mock_hook):
        op = CloudVisionImageAnnotateOperator(request=ANNOTATE_REQUEST_TEST, task_id='id')
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.annotate_image.assert_called_once_with(
            request=ANNOTATE_REQUEST_TEST, retry=None, timeout=None
        )

    @mock.patch('airflow.providers.google.cloud.operators.vision.CloudVisionHook')
    def test_minimal_green_path_for_batch(self, mock_hook):
        op = CloudVisionImageAnnotateOperator(request=ANNOTATE_REQUEST_BATCH_TEST, task_id='id')
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.batch_annotate_images.assert_called_once_with(
            requests=ANNOTATE_REQUEST_BATCH_TEST, retry=None, timeout=None
        )


class TestCloudVisionDetectTextOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.vision.CloudVisionHook")
    def test_minimal_green_path(self, mock_hook):
        op = CloudVisionDetectTextOperator(image=DETECT_TEST_IMAGE, task_id="id")
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.text_detection.assert_called_once_with(
            image=DETECT_TEST_IMAGE, max_results=None, retry=None, timeout=None, additional_properties=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.vision.CloudVisionHook")
    def test_additional_params(self, mock_hook):
        op = CloudVisionDetectTextOperator(
            image=DETECT_TEST_IMAGE,
            task_id="id",
            language_hints="pl",
            web_detection_params={'param': 'test'},
            additional_properties={
                'image_context': {'additional_property_1': 'add_1'},
                'additional_property_2': 'add_2',
            },
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.text_detection.assert_called_once_with(
            image=DETECT_TEST_IMAGE,
            max_results=None,
            retry=None,
            timeout=None,
            additional_properties={
                'additional_property_2': 'add_2',
                'image_context': {
                    'language_hints': 'pl',
                    'additional_property_1': 'add_1',
                    'web_detection_params': {'param': 'test'},
                },
            },
        )


class TestCloudVisionDetectDocumentTextOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.vision.CloudVisionHook")
    def test_minimal_green_path(self, mock_hook):
        op = CloudVisionTextDetectOperator(image=DETECT_TEST_IMAGE, task_id="id")
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.document_text_detection.assert_called_once_with(
            image=DETECT_TEST_IMAGE, max_results=None, retry=None, timeout=None, additional_properties=None
        )


class TestCloudVisionDetectImageLabelsOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.vision.CloudVisionHook")
    def test_minimal_green_path(self, mock_hook):
        op = CloudVisionDetectImageLabelsOperator(image=DETECT_TEST_IMAGE, task_id="id")
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.label_detection.assert_called_once_with(
            image=DETECT_TEST_IMAGE, max_results=None, retry=None, timeout=None, additional_properties=None
        )


class TestCloudVisionDetectImageSafeSearchOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.vision.CloudVisionHook")
    def test_minimal_green_path(self, mock_hook):
        op = CloudVisionDetectImageSafeSearchOperator(image=DETECT_TEST_IMAGE, task_id="id")
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.safe_search_detection.assert_called_once_with(
            image=DETECT_TEST_IMAGE, max_results=None, retry=None, timeout=None, additional_properties=None
        )
