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
from google.api_core.exceptions import AlreadyExists

from airflow.contrib.hooks.gcp_vision_hook import CloudVisionHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CloudVisionProductSetCreateOperator(BaseOperator):
    """
    Creates a new ProductSet resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductSetCreateOperator`

    :param product_set: (Required) The ProductSet to create. If a dict is provided, it must be of the same
        form as the protobuf message `ProductSet`.
    :type product_set: dict or google.cloud.vision_v1.types.ProductSet
    :param location: (Required) The region where the ProductSet should be created. Valid regions
        (as of 2019-02-05) are: us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param project_id: (Optional) The project in which the ProductSet should be created. If set to None or
        missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param product_set_id: (Optional) A user-supplied resource id for this ProductSet.
        If set, the server will attempt to use this value as the resource id. If it is
        already in use, an error is returned with code ALREADY_EXISTS. Must be at most
        128 characters long. It cannot contain the character /.
    :type product_set_id: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START vision_productset_create_template_fields]
    template_fields = ('location', 'project_id', 'product_set_id', 'gcp_conn_id')
    # [END vision_productset_create_template_fields]

    @apply_defaults
    def __init__(
        self,
        product_set,
        location,
        project_id=None,
        product_set_id=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductSetCreateOperator, self).__init__(*args, **kwargs)
        self.location = location
        self.project_id = project_id
        self.product_set = product_set
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        try:
            return self._hook.create_product_set(
                location=self.location,
                project_id=self.project_id,
                product_set=self.product_set,
                product_set_id=self.product_set_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info(
                'Product set with id %s already exists. Exiting from the create operation.',
                self.product_set_id,
            )
            return self.product_set_id


class CloudVisionProductSetGetOperator(BaseOperator):
    """
    Gets information associated with a ProductSet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductSetGetOperator`

    :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
        are: us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param product_set_id: (Required) The resource id of this ProductSet.
    :type product_set_id: str
    :param project_id: (Optional) The project in which the ProductSet is located. If set
        to None or missing, the default `project_id` from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START vision_productset_get_template_fields]
    template_fields = ('location', 'project_id', 'product_set_id', 'gcp_conn_id')
    # [END vision_productset_get_template_fields]

    @apply_defaults
    def __init__(
        self,
        location,
        product_set_id,
        project_id=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductSetGetOperator, self).__init__(*args, **kwargs)
        self.location = location
        self.project_id = project_id
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        return self._hook.get_product_set(
            location=self.location,
            product_set_id=self.product_set_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionProductSetUpdateOperator(BaseOperator):
    """
    Makes changes to a `ProductSet` resource. Only display_name can be updated currently.

    .. note:: To locate the `ProductSet` resource, its `name` in the form
        `projects/PROJECT_ID/locations/LOC_ID/productSets/PRODUCT_SET_ID` is necessary.

    You can provide the `name` directly as an attribute of the `product_set` object.
    However, you can leave it blank and provide `location` and `product_set_id` instead
    (and optionally `project_id` - if not present, the connection default will be used)
    and the `name` will be created by the operator itself.

    This mechanism exists for your convenience, to allow leaving the `project_id` empty
    and having Airflow use the connection default `project_id`.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductSetUpdateOperator`

    :param product_set: (Required) The ProductSet resource which replaces the one on the
        server. If a dict is provided, it must be of the same form as the protobuf
        message `ProductSet`.
    :type product_set: dict or google.cloud.vision_v1.types.ProductSet
    :param location: (Optional) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
        are: us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param product_set_id: (Optional) The resource id of this ProductSet.
    :type product_set_id: str
    :param project_id: (Optional) The project in which the ProductSet should be created. If set to None or
        missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param update_mask: (Optional) The `FieldMask` that specifies which fields to update. If update_mask
        isn’t specified, all mutable fields are to be updated. Valid mask path is display_name. If a dict is
        provided, it must be of the same form as the protobuf message `FieldMask`.
    :type update_mask: dict or google.cloud.vision_v1.types.FieldMask
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str

    """

    # [START vision_productset_update_template_fields]
    template_fields = ('location', 'project_id', 'product_set_id', 'gcp_conn_id')
    # [END vision_productset_update_template_fields]

    @apply_defaults
    def __init__(
        self,
        product_set,
        location=None,
        product_set_id=None,
        project_id=None,
        update_mask=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductSetUpdateOperator, self).__init__(*args, **kwargs)
        self.product_set = product_set
        self.update_mask = update_mask
        self.location = location
        self.project_id = project_id
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        return self._hook.update_product_set(
            location=self.location,
            product_set_id=self.product_set_id,
            project_id=self.project_id,
            product_set=self.product_set,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionProductSetDeleteOperator(BaseOperator):
    """
    Permanently deletes a `ProductSet`. `Products` and `ReferenceImages` in the
    `ProductSet` are not deleted. The actual image files are not deleted from Google
    Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductSetDeleteOperator`

    :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
        are: us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param product_set_id: (Required) The resource id of this ProductSet.
    :type product_set_id: str
    :param project_id: (Optional) The project in which the ProductSet should be created.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str

    """

    # [START vision_productset_delete_template_fields]
    template_fields = ('location', 'project_id', 'product_set_id', 'gcp_conn_id')
    # [END vision_productset_delete_template_fields]

    @apply_defaults
    def __init__(
        self,
        location,
        product_set_id,
        project_id=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductSetDeleteOperator, self).__init__(*args, **kwargs)
        self.location = location
        self.project_id = project_id
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        return self._hook.delete_product_set(
            location=self.location,
            product_set_id=self.product_set_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionProductCreateOperator(BaseOperator):
    """
    Creates and returns a new product resource.

    Possible errors regarding the `Product` object provided:

    - Returns INVALID_ARGUMENT if `display_name` is missing or longer than 4096 characters.
    - Returns INVALID_ARGUMENT if `description` is longer than 4096 characters.
    - Returns INVALID_ARGUMENT if `product_category` is missing or invalid.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductCreateOperator`

    :param location: (Required) The region where the Product should be created. Valid regions
        (as of 2019-02-05) are: us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param product: (Required) The product to create. If a dict is provided, it must be of the same form as
        the protobuf message `Product`.
    :type product: dict or google.cloud.vision_v1.types.Product
    :param project_id: (Optional) The project in which the Product should be created. If set to None or
        missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param product_id: (Optional) A user-supplied resource id for this Product.
        If set, the server will attempt to use this value as the resource id. If it is
        already in use, an error is returned with code ALREADY_EXISTS. Must be at most
        128 characters long. It cannot contain the character /.
    :type product_id: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str

    """

    # [START vision_product_create_template_fields]
    template_fields = ('location', 'project_id', 'product_id', 'gcp_conn_id')
    # [END vision_product_create_template_fields]

    @apply_defaults
    def __init__(
        self,
        location,
        product,
        project_id=None,
        product_id=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductCreateOperator, self).__init__(*args, **kwargs)
        self.location = location
        self.product = product
        self.project_id = project_id
        self.product_id = product_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        try:
            return self._hook.create_product(
                location=self.location,
                product=self.product,
                project_id=self.project_id,
                product_id=self.product_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info(
                'Product with id %s already exists. Exiting from the create operation.', self.product_id
            )
            return self.product_id


class CloudVisionProductGetOperator(BaseOperator):
    """
    Gets information associated with a `Product`.

    Possible errors:

    - Returns NOT_FOUND if the `Product` does not exist.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductGetOperator`

    :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param product_id: (Required) The resource id of this Product.
    :type product_id: str
    :param project_id: (Optional) The project in which the Product is located. If set to
        None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str

    """

    # [START vision_product_get_template_fields]
    template_fields = ('location', 'project_id', 'product_id', 'gcp_conn_id')
    # [END vision_product_get_template_fields]

    @apply_defaults
    def __init__(
        self,
        location,
        product_id,
        project_id=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductGetOperator, self).__init__(*args, **kwargs)
        self.location = location
        self.product_id = product_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        return self._hook.get_product(
            location=self.location,
            product_id=self.product_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionProductUpdateOperator(BaseOperator):
    """
    Makes changes to a Product resource. Only the display_name, description, and labels fields can be
    updated right now.

    If labels are updated, the change will not be reflected in queries until the next index time.

    .. note:: To locate the `Product` resource, its `name` in the form
        `projects/PROJECT_ID/locations/LOC_ID/products/PRODUCT_ID` is necessary.

    You can provide the `name` directly as an attribute of the `product` object. However, you can leave it
    blank and provide `location` and `product_id` instead (and optionally `project_id` - if not present,
    the connection default will be used) and the `name` will be created by the operator itself.

    This mechanism exists for your convenience, to allow leaving the `project_id` empty and having Airflow
    use the connection default `project_id`.

    Possible errors related to the provided `Product`:

    - Returns NOT_FOUND if the Product does not exist.
    - Returns INVALID_ARGUMENT if display_name is present in update_mask but is missing from the request or
        longer than 4096 characters.
    - Returns INVALID_ARGUMENT if description is present in update_mask but is longer than 4096 characters.
    - Returns INVALID_ARGUMENT if product_category is present in update_mask.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductUpdateOperator`

    :param product: (Required) The Product resource which replaces the one on the server. product.name is
        immutable. If a dict is provided, it must be of the same form as the protobuf message `Product`.
    :type product: dict or google.cloud.vision_v1.types.ProductSet
    :param location: (Optional) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param product_id: (Optional) The resource id of this Product.
    :type product_id: str
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param update_mask: (Optional) The `FieldMask` that specifies which fields to update. If update_mask
        isn’t specified, all mutable fields are to be updated. Valid mask paths include product_labels,
        display_name, and description. If a dict is provided, it must be of the same form as the protobuf
        message `FieldMask`.
    :type update_mask: dict or google.cloud.vision_v1.types.FieldMask
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START vision_product_update_template_fields]
    template_fields = ('location', 'project_id', 'product_id', 'gcp_conn_id')
    # [END vision_product_update_template_fields]

    @apply_defaults
    def __init__(
        self,
        product,
        location=None,
        product_id=None,
        project_id=None,
        update_mask=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductUpdateOperator, self).__init__(*args, **kwargs)
        self.product = product
        self.location = location
        self.product_id = product_id
        self.project_id = project_id
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        return self._hook.update_product(
            product=self.product,
            location=self.location,
            product_id=self.product_id,
            project_id=self.project_id,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionProductDeleteOperator(BaseOperator):
    """
    Permanently deletes a product and its reference images.

    Metadata of the product and all its images will be deleted right away, but search queries against
    ProductSets containing the product may still work until all related caches are refreshed.

    Possible errors:

    - Returns NOT_FOUND if the product does not exist.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionProductDeleteOperator`

    :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :type location: str
    :param product_id: (Required) The resource id of this Product.
    :type product_id: str
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START vision_product_delete_template_fields]
    template_fields = ('location', 'project_id', 'product_id', 'gcp_conn_id')
    # [END vision_product_delete_template_fields]

    @apply_defaults
    def __init__(
        self,
        location,
        product_id,
        project_id=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super(CloudVisionProductDeleteOperator, self).__init__(*args, **kwargs)
        self.location = location
        self.product_id = product_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self._hook = CloudVisionHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context):
        return self._hook.delete_product(
            location=self.location,
            product_id=self.product_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
