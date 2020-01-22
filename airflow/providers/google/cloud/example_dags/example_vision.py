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

"""
Example Airflow DAG that creates, gets, updates and deletes Products and Product Sets in the Google Cloud
Vision service in the Google Cloud Platform.

This DAG relies on the following OS environment variables

* GCP_VISION_LOCATION - Zone where the instance exists.
* GCP_VISION_PRODUCT_SET_ID - Product Set ID.
* GCP_VISION_PRODUCT_ID - Product  ID.
* GCP_VISION_REFERENCE_IMAGE_ID - Reference Image ID.
* GCP_VISION_REFERENCE_IMAGE_URL - A link to the bucket that contains the reference image.
* GCP_VISION_ANNOTATE_IMAGE_URL - A link to the bucket that contains the file to be annotated.

"""

import os

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.vision import (
    CloudVisionAddProductToProductSetOperator, CloudVisionCreateProductOperator,
    CloudVisionCreateProductSetOperator, CloudVisionCreateReferenceImageOperator,
    CloudVisionDeleteProductOperator, CloudVisionDeleteProductSetOperator,
    CloudVisionDetectImageLabelsOperator, CloudVisionDetectImageSafeSearchOperator,
    CloudVisionDetectTextOperator, CloudVisionGetProductOperator, CloudVisionGetProductSetOperator,
    CloudVisionImageAnnotateOperator, CloudVisionRemoveProductFromProductSetOperator,
    CloudVisionTextDetectOperator, CloudVisionUpdateProductOperator, CloudVisionUpdateProductSetOperator,
)
from airflow.utils.dates import days_ago

# [START howto_operator_vision_retry_import]
from google.api_core.retry import Retry  # isort:skip pylint: disable=wrong-import-order
# [END howto_operator_vision_retry_import]
# [START howto_operator_vision_product_set_import]
from google.cloud.vision_v1.types import ProductSet  # isort:skip pylint: disable=wrong-import-order
# [END howto_operator_vision_product_set_import]
# [START howto_operator_vision_product_import]
from google.cloud.vision_v1.types import Product  # isort:skip pylint: disable=wrong-import-order
# [END howto_operator_vision_product_import]
# [START howto_operator_vision_reference_image_import]
from google.cloud.vision_v1.types import ReferenceImage  # isort:skip pylint: disable=wrong-import-order
# [END howto_operator_vision_reference_image_import]
# [START howto_operator_vision_enums_import]
from google.cloud.vision import enums  # isort:skip pylint: disable=wrong-import-order
# [END howto_operator_vision_enums_import]


default_args = {'start_date': days_ago(1)}

# [START howto_operator_vision_args_common]
GCP_VISION_LOCATION = os.environ.get('GCP_VISION_LOCATION', 'europe-west1')
# [END howto_operator_vision_args_common]

# [START howto_operator_vision_product_set_explicit_id]
GCP_VISION_PRODUCT_SET_ID = os.environ.get('GCP_VISION_PRODUCT_SET_ID', 'product_set_explicit_id')
# [END howto_operator_vision_product_set_explicit_id]

# [START howto_operator_vision_product_explicit_id]
GCP_VISION_PRODUCT_ID = os.environ.get('GCP_VISION_PRODUCT_ID', 'product_explicit_id')
# [END howto_operator_vision_product_explicit_id]

# [START howto_operator_vision_reference_image_args]
GCP_VISION_REFERENCE_IMAGE_ID = os.environ.get('GCP_VISION_REFERENCE_IMAGE_ID', 'reference_image_explicit_id')
GCP_VISION_REFERENCE_IMAGE_URL = os.environ.get('GCP_VISION_REFERENCE_IMAGE_URL', 'gs://bucket/image1.jpg')
# [END howto_operator_vision_reference_image_args]

# [START howto_operator_vision_annotate_image_url]
GCP_VISION_ANNOTATE_IMAGE_URL = os.environ.get('GCP_VISION_ANNOTATE_IMAGE_URL', 'gs://bucket/image2.jpg')
# [END howto_operator_vision_annotate_image_url]

# [START howto_operator_vision_product_set]
product_set = ProductSet(display_name='My Product Set')
# [END howto_operator_vision_product_set]

# [START howto_operator_vision_product]
product = Product(display_name='My Product 1', product_category='toys')
# [END howto_operator_vision_product]

# [START howto_operator_vision_reference_image]
reference_image = ReferenceImage(uri=GCP_VISION_REFERENCE_IMAGE_URL)
# [END howto_operator_vision_reference_image]

# [START howto_operator_vision_annotate_image_request]
annotate_image_request = {
    'image': {'source': {'image_uri': GCP_VISION_ANNOTATE_IMAGE_URL}},
    'features': [{'type': enums.Feature.Type.LOGO_DETECTION}],
}
# [END howto_operator_vision_annotate_image_request]

# [START howto_operator_vision_detect_image_param]
DETECT_IMAGE = {"source": {"image_uri": GCP_VISION_ANNOTATE_IMAGE_URL}}
# [END howto_operator_vision_detect_image_param]

with models.DAG(
    'example_gcp_vision_autogenerated_id', default_args=default_args, schedule_interval=None
) as dag_autogenerated_id:
    # ################################## #
    # ### Autogenerated IDs examples ### #
    # ################################## #

    # [START howto_operator_vision_product_set_create]
    product_set_create = CloudVisionCreateProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set=product_set,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='product_set_create',
    )
    # [END howto_operator_vision_product_set_create]

    # [START howto_operator_vision_product_set_get]
    product_set_get = CloudVisionGetProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id="{{ task_instance.xcom_pull('product_set_create') }}",
        task_id='product_set_get',
    )
    # [END howto_operator_vision_product_set_get]

    # [START howto_operator_vision_product_set_update]
    product_set_update = CloudVisionUpdateProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id="{{ task_instance.xcom_pull('product_set_create') }}",
        product_set=ProductSet(display_name='My Product Set 2'),
        task_id='product_set_update',
    )
    # [END howto_operator_vision_product_set_update]

    # [START howto_operator_vision_product_set_delete]
    product_set_delete = CloudVisionDeleteProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id="{{ task_instance.xcom_pull('product_set_create') }}",
        task_id='product_set_delete',
    )
    # [END howto_operator_vision_product_set_delete]

    # [START howto_operator_vision_product_create]
    product_create = CloudVisionCreateProductOperator(
        location=GCP_VISION_LOCATION,
        product=product,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='product_create',
    )
    # [END howto_operator_vision_product_create]

    # [START howto_operator_vision_product_get]
    product_get = CloudVisionGetProductOperator(
        location=GCP_VISION_LOCATION,
        product_id="{{ task_instance.xcom_pull('product_create') }}",
        task_id='product_get',
    )
    # [END howto_operator_vision_product_get]

    # [START howto_operator_vision_product_update]
    product_update = CloudVisionUpdateProductOperator(
        location=GCP_VISION_LOCATION,
        product_id="{{ task_instance.xcom_pull('product_create') }}",
        product=Product(display_name='My Product 2', description='My updated description'),
        task_id='product_update',
    )
    # [END howto_operator_vision_product_update]

    # [START howto_operator_vision_product_delete]
    product_delete = CloudVisionDeleteProductOperator(
        location=GCP_VISION_LOCATION,
        product_id="{{ task_instance.xcom_pull('product_create') }}",
        task_id='product_delete',
    )
    # [END howto_operator_vision_product_delete]

    # [START howto_operator_vision_reference_image_create]
    reference_image_create = CloudVisionCreateReferenceImageOperator(
        location=GCP_VISION_LOCATION,
        reference_image=reference_image,
        product_id="{{ task_instance.xcom_pull('product_create') }}",
        reference_image_id=GCP_VISION_REFERENCE_IMAGE_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='reference_image_create',
    )
    # [END howto_operator_vision_reference_image_create]

    # [START howto_operator_vision_add_product_to_product_set]
    add_product_to_product_set = CloudVisionAddProductToProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id="{{ task_instance.xcom_pull('product_set_create') }}",
        product_id="{{ task_instance.xcom_pull('product_create') }}",
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='add_product_to_product_set',
    )
    # [END howto_operator_vision_add_product_to_product_set]

    # [START howto_operator_vision_remove_product_from_product_set]
    remove_product_from_product_set = CloudVisionRemoveProductFromProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id="{{ task_instance.xcom_pull('product_set_create') }}",
        product_id="{{ task_instance.xcom_pull('product_create') }}",
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='remove_product_from_product_set',
    )
    # [END howto_operator_vision_remove_product_from_product_set]

    # Product path
    product_create >> product_get >> product_update >> product_delete

    # ProductSet path
    product_set_create >> product_set_get >> product_set_update >> product_set_delete

    # ReferenceImage path
    product_create >> reference_image_create >> product_delete

    # Product/ProductSet path
    product_create >> add_product_to_product_set
    product_set_create >> add_product_to_product_set
    add_product_to_product_set >> remove_product_from_product_set
    remove_product_from_product_set >> product_delete
    remove_product_from_product_set >> product_set_delete

with models.DAG(
    'example_gcp_vision_explicit_id', default_args=default_args, schedule_interval=None
) as dag_explicit_id:
    # ############################# #
    # ### Explicit IDs examples ### #
    # ############################# #

    # [START howto_operator_vision_product_set_create_2]
    product_set_create_2 = CloudVisionCreateProductSetOperator(
        product_set_id=GCP_VISION_PRODUCT_SET_ID,
        location=GCP_VISION_LOCATION,
        product_set=product_set,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='product_set_create_2',
    )
    # [END howto_operator_vision_product_set_create_2]

    # Second 'create' task with the same product_set_id to demonstrate idempotence
    product_set_create_2_idempotence = CloudVisionCreateProductSetOperator(
        product_set_id=GCP_VISION_PRODUCT_SET_ID,
        location=GCP_VISION_LOCATION,
        product_set=product_set,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='product_set_create_2_idempotence',
    )

    # [START howto_operator_vision_product_set_get_2]
    product_set_get_2 = CloudVisionGetProductSetOperator(
        location=GCP_VISION_LOCATION, product_set_id=GCP_VISION_PRODUCT_SET_ID, task_id='product_set_get_2'
    )
    # [END howto_operator_vision_product_set_get_2]

    # [START howto_operator_vision_product_set_update_2]
    product_set_update_2 = CloudVisionUpdateProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id=GCP_VISION_PRODUCT_SET_ID,
        product_set=ProductSet(display_name='My Product Set 2'),
        task_id='product_set_update_2',
    )
    # [END howto_operator_vision_product_set_update_2]

    # [START howto_operator_vision_product_set_delete_2]
    product_set_delete_2 = CloudVisionDeleteProductSetOperator(
        location=GCP_VISION_LOCATION, product_set_id=GCP_VISION_PRODUCT_SET_ID, task_id='product_set_delete_2'
    )
    # [END howto_operator_vision_product_set_delete_2]

    # [START howto_operator_vision_product_create_2]
    product_create_2 = CloudVisionCreateProductOperator(
        product_id=GCP_VISION_PRODUCT_ID,
        location=GCP_VISION_LOCATION,
        product=product,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='product_create_2',
    )
    # [END howto_operator_vision_product_create_2]

    # Second 'create' task with the same product_id to demonstrate idempotence
    product_create_2_idempotence = CloudVisionCreateProductOperator(
        product_id=GCP_VISION_PRODUCT_ID,
        location=GCP_VISION_LOCATION,
        product=product,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='product_create_2_idempotence',
    )

    # [START howto_operator_vision_product_get_2]
    product_get_2 = CloudVisionGetProductOperator(
        location=GCP_VISION_LOCATION, product_id=GCP_VISION_PRODUCT_ID, task_id='product_get_2'
    )
    # [END howto_operator_vision_product_get_2]

    # [START howto_operator_vision_product_update_2]
    product_update_2 = CloudVisionUpdateProductOperator(
        location=GCP_VISION_LOCATION,
        product_id=GCP_VISION_PRODUCT_ID,
        product=Product(display_name='My Product 2', description='My updated description'),
        task_id='product_update_2',
    )
    # [END howto_operator_vision_product_update_2]

    # [START howto_operator_vision_product_delete_2]
    product_delete_2 = CloudVisionDeleteProductOperator(
        location=GCP_VISION_LOCATION, product_id=GCP_VISION_PRODUCT_ID, task_id='product_delete_2'
    )
    # [END howto_operator_vision_product_delete_2]

    # [START howto_operator_vision_reference_image_create_2]
    reference_image_create_2 = CloudVisionCreateReferenceImageOperator(
        location=GCP_VISION_LOCATION,
        reference_image=reference_image,
        product_id=GCP_VISION_PRODUCT_ID,
        reference_image_id=GCP_VISION_REFERENCE_IMAGE_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='reference_image_create_2',
    )
    # [END howto_operator_vision_reference_image_create_2]

    # Second 'create' task with the same product_id to demonstrate idempotence
    reference_image_create_2_idempotence = CloudVisionCreateReferenceImageOperator(
        location=GCP_VISION_LOCATION,
        reference_image=reference_image,
        product_id=GCP_VISION_PRODUCT_ID,
        reference_image_id=GCP_VISION_REFERENCE_IMAGE_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='reference_image_create_2_idempotence',
    )

    # [START howto_operator_vision_add_product_to_product_set_2]
    add_product_to_product_set_2 = CloudVisionAddProductToProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id=GCP_VISION_PRODUCT_SET_ID,
        product_id=GCP_VISION_PRODUCT_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='add_product_to_product_set_2',
    )
    # [END howto_operator_vision_add_product_to_product_set_2]

    # [START howto_operator_vision_remove_product_from_product_set_2]
    remove_product_from_product_set_2 = CloudVisionRemoveProductFromProductSetOperator(
        location=GCP_VISION_LOCATION,
        product_set_id=GCP_VISION_PRODUCT_SET_ID,
        product_id=GCP_VISION_PRODUCT_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id='remove_product_from_product_set_2',
    )
    # [END howto_operator_vision_remove_product_from_product_set_2]

    # Product path
    product_create_2 >> product_create_2_idempotence >> product_get_2 >> product_update_2 >> product_delete_2

    # ProductSet path
    product_set_create_2 >> product_set_get_2 >> product_set_update_2 >> product_set_delete_2
    product_set_create_2 >> product_set_create_2_idempotence >> product_set_delete_2

    # ReferenceImage path
    product_create_2 >> reference_image_create_2 >> reference_image_create_2_idempotence >> product_delete_2

    # Product/ProductSet path
    add_product_to_product_set_2 >> remove_product_from_product_set_2
    product_set_create_2 >> add_product_to_product_set_2
    product_create_2 >> add_product_to_product_set_2
    remove_product_from_product_set_2 >> product_set_delete_2
    remove_product_from_product_set_2 >> product_delete_2

with models.DAG(
    'example_gcp_vision_annotate_image', default_args=default_args, schedule_interval=None
) as dag_annotate_image:
    # ############################## #
    # ### Annotate image example ### #
    # ############################## #

    # [START howto_operator_vision_annotate_image]
    annotate_image = CloudVisionImageAnnotateOperator(
        request=annotate_image_request, retry=Retry(maximum=10.0), timeout=5, task_id='annotate_image'
    )
    # [END howto_operator_vision_annotate_image]

    # [START howto_operator_vision_annotate_image_result]
    annotate_image_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('annotate_image')"
        "['logoAnnotations'][0]['description'] }}",
        task_id='annotate_image_result',
    )
    # [END howto_operator_vision_annotate_image_result]

    # [START howto_operator_vision_detect_text]
    detect_text = CloudVisionDetectTextOperator(
        image=DETECT_IMAGE,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="detect_text",
        language_hints="en",
        web_detection_params={'include_geo_results': True},
    )
    # [END howto_operator_vision_detect_text]

    # [START howto_operator_vision_detect_text_result]
    detect_text_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_text')['textAnnotations'][0] }}",
        task_id="detect_text_result",
    )
    # [END howto_operator_vision_detect_text_result]

    # [START howto_operator_vision_document_detect_text]
    document_detect_text = CloudVisionTextDetectOperator(
        image=DETECT_IMAGE, retry=Retry(maximum=10.0), timeout=5, task_id="document_detect_text"
    )
    # [END howto_operator_vision_document_detect_text]

    # [START howto_operator_vision_document_detect_text_result]
    document_detect_text_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('document_detect_text')['textAnnotations'][0] }}",
        task_id="document_detect_text_result",
    )
    # [END howto_operator_vision_document_detect_text_result]

    # [START howto_operator_vision_detect_labels]
    detect_labels = CloudVisionDetectImageLabelsOperator(
        image=DETECT_IMAGE, retry=Retry(maximum=10.0), timeout=5, task_id="detect_labels"
    )
    # [END howto_operator_vision_detect_labels]

    # [START howto_operator_vision_detect_labels_result]
    detect_labels_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_labels')['labelAnnotations'][0] }}",
        task_id="detect_labels_result",
    )
    # [END howto_operator_vision_detect_labels_result]

    # [START howto_operator_vision_detect_safe_search]
    detect_safe_search = CloudVisionDetectImageSafeSearchOperator(
        image=DETECT_IMAGE, retry=Retry(maximum=10.0), timeout=5, task_id="detect_safe_search"
    )
    # [END howto_operator_vision_detect_safe_search]

    # [START howto_operator_vision_detect_safe_search_result]
    detect_safe_search_result = BashOperator(
        bash_command="echo {{ task_instance.xcom_pull('detect_safe_search') }}",
        task_id="detect_safe_search_result",
    )
    # [END howto_operator_vision_detect_safe_search_result]

    annotate_image >> annotate_image_result

    detect_text >> detect_text_result
    document_detect_text >> document_detect_text_result
    detect_labels >> detect_labels_result
    detect_safe_search >> detect_safe_search_result
