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
"""This module contains a Google Cloud Vision operator."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any, Sequence, Tuple

from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.vision_v1 import (
    AnnotateImageRequest,
    Image,
    Product,
    ProductSet,
    ReferenceImage,
)

from airflow.providers.google.cloud.hooks.vision import CloudVisionHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.utils.context import Context


MetaData = Sequence[Tuple[str, str]]


class CloudVisionCreateProductSetOperator(GoogleCloudBaseOperator):
    """
    Create a new ProductSet resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionCreateProductSetOperator`

    :param product_set: (Required) The ProductSet to create. If a dict is provided, it must be of the same
        form as the protobuf message `ProductSet`.
    :param location: (Required) The region where the ProductSet should be created. Valid regions
        (as of 2019-02-05) are: us-east1, us-west1, europe-west1, asia-east1
    :param project_id: (Optional) The project in which the ProductSet should be created. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param product_set_id: (Optional) A user-supplied resource id for this ProductSet.
        If set, the server will attempt to use this value as the resource id. If it is
        already in use, an error is returned with code ALREADY_EXISTS. Must be at most
        128 characters long. It cannot contain the character /.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_productset_create_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_set_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_productset_create_template_fields]

    def __init__(
        self,
        *,
        product_set: dict | ProductSet,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        product_set_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.product_set = product_set
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            return hook.create_product_set(
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
                "Product set with id %s already exists. Exiting from the create operation.",
                self.product_set_id,
            )
            return self.product_set_id


class CloudVisionGetProductSetOperator(GoogleCloudBaseOperator):
    """
    Get information associated with a ProductSet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionGetProductSetOperator`

    :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
        are: us-east1, us-west1, europe-west1, asia-east1
    :param product_set_id: (Required) The resource id of this ProductSet.
    :param project_id: (Optional) The project in which the ProductSet is located. If set
        to None or missing, the default `project_id` from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_productset_get_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_set_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_productset_get_template_fields]

    def __init__(
        self,
        *,
        location: str,
        product_set_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.get_product_set(
            location=self.location,
            product_set_id=self.product_set_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionUpdateProductSetOperator(GoogleCloudBaseOperator):
    """
    Make changes to a `ProductSet` resource.

    Only ``display_name`` can be updated currently.

    .. note:: To locate the ``ProductSet`` resource, its ``name`` in the form
        `projects/PROJECT_ID/locations/LOC_ID/productSets/PRODUCT_SET_ID` is necessary.

    You can provide the ``name` directly as an attribute of the ``product_set``
    object. You can also leave it blank, in which case ``name`` will be created
    by the operator from ``location`` and ``product_set_id`` instead (and
    optionally ``project_id``; if not present, the connection default will be
    used).

    This mechanism exists for your convenience, to allow leaving the
    ``project_id`` empty and having Airflow use the connection default.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionUpdateProductSetOperator`

    :param product_set: (Required) The ProductSet resource which replaces the one on the
        server. If a dict is provided, it must be of the same form as the protobuf
        message `ProductSet`.
    :param location: (Optional) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
        are: us-east1, us-west1, europe-west1, asia-east1
    :param product_set_id: (Optional) The resource id of this ProductSet.
    :param project_id: (Optional) The project in which the ProductSet should be created. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param update_mask: (Optional) The `FieldMask` that specifies which fields to update. If update_mask
        isn't specified, all mutable fields are to be updated. Valid mask path is display_name. If a dict is
        provided, it must be of the same form as the protobuf message `FieldMask`.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_productset_update_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_set_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_productset_update_template_fields]

    def __init__(
        self,
        *,
        product_set: dict | ProductSet,
        location: str | None = None,
        product_set_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        update_mask: dict | FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.product_set = product_set
        self.update_mask = update_mask
        self.location = location
        self.project_id = project_id
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        if isinstance(self.product_set, dict):
            self.product_set = ProductSet(self.product_set)
        return hook.update_product_set(
            location=self.location,
            product_set_id=self.product_set_id,
            project_id=self.project_id,
            product_set=self.product_set,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionDeleteProductSetOperator(GoogleCloudBaseOperator):
    """
    Permanently deletes a ``ProductSet``.

    ``Products`` and ``ReferenceImages`` in the ``ProductSet`` are not deleted.
    The actual image files are not deleted from Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionDeleteProductSetOperator`

    :param location: (Required) The region where the ProductSet is located.
        Valid regions (as of 2019-02-05) are: us-east1, us-west1, europe-west1, asia-east1
    :param product_set_id: (Required) The resource id of this ProductSet.
    :param project_id: (Optional) The project in which the ProductSet should be created.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_productset_delete_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_set_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_productset_delete_template_fields]

    def __init__(
        self,
        *,
        location: str,
        product_set_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.product_set_id = product_set_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_product_set(
            location=self.location,
            product_set_id=self.product_set_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionCreateProductOperator(GoogleCloudBaseOperator):
    """
    Create and return a new product resource.

    Possible errors regarding the ``Product`` object provided:

    - Returns ``INVALID_ARGUMENT`` if ``display_name`` is missing or longer than 4096 characters.
    - Returns ``INVALID_ARGUMENT`` if ``description`` is longer than 4096 characters.
    - Returns ``INVALID_ARGUMENT`` if ``product_category`` is missing or invalid.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionCreateProductOperator`

    :param location: (Required) The region where the Product should be created. Valid regions
        (as of 2019-02-05) are: us-east1, us-west1, europe-west1, asia-east1
    :param product: (Required) The product to create. If a dict is provided, it must be of the same form as
        the protobuf message `Product`.
    :param project_id: (Optional) The project in which the Product should be created. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param product_id: (Optional) A user-supplied resource id for this Product.
        If set, the server will attempt to use this value as the resource id. If it is
        already in use, an error is returned with code ALREADY_EXISTS. Must be at most
        128 characters long. It cannot contain the character /.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_product_create_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_product_create_template_fields]

    def __init__(
        self,
        *,
        location: str,
        product: str,
        project_id: str = PROVIDE_PROJECT_ID,
        product_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.product = product
        self.project_id = project_id
        self.product_id = product_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            return hook.create_product(
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
                "Product with id %s already exists. Exiting from the create operation.",
                self.product_id,
            )
            return self.product_id


class CloudVisionGetProductOperator(GoogleCloudBaseOperator):
    """
    Get information associated with a ``Product``.

    Possible errors:

    - Returns `NOT_FOUND` if the `Product` does not exist.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionGetProductOperator`

    :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :param product_id: (Required) The resource id of this Product.
    :param project_id: (Optional) The project in which the Product is located. If set to
        None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_product_get_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_product_get_template_fields]

    def __init__(
        self,
        *,
        location: str,
        product_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.product_id = product_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.get_product(
            location=self.location,
            product_id=self.product_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionUpdateProductOperator(GoogleCloudBaseOperator):
    """
    Make changes to a Product resource.

    Only the display_name, description, and labels fields can be updated right now.

    If labels are updated, the change will not be reflected in queries until the next index time.

    .. note:: To locate the `Product` resource, its `name` in the form
        `projects/PROJECT_ID/locations/LOC_ID/products/PRODUCT_ID` is necessary.

    You can provide the `name` directly as an attribute of the `product` object. However, you can leave it
    blank and provide `location` and `product_id` instead (and optionally `project_id` - if not present,
    the connection default will be used) and the `name` will be created by the operator itself.

    This mechanism exists for your convenience, to allow leaving the `project_id` empty and having Airflow
    use the connection default `project_id`.

    Possible errors related to the provided `Product`:

    - Returns `NOT_FOUND` if the Product does not exist.
    - Returns `INVALID_ARGUMENT` if `display_name` is present in update_mask but is missing from the request
        or longer than 4096 characters.
    - Returns `INVALID_ARGUMENT` if `description` is present in update_mask but is longer than 4096
        characters.
    - Returns `INVALID_ARGUMENT` if `product_category` is present in update_mask.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionUpdateProductOperator`

    :param product: (Required) The Product resource which replaces the one on the server. product.name is
        immutable. If a dict is provided, it must be of the same form as the protobuf message `Product`.
    :param location: (Optional) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :param product_id: (Optional) The resource id of this Product.
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param update_mask: (Optional) The `FieldMask` that specifies which fields to update. If update_mask
        isn't specified, all mutable fields are to be updated. Valid mask paths include product_labels,
        display_name, and description. If a dict is provided, it must be of the same form as the protobuf
        message `FieldMask`.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_product_update_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_product_update_template_fields]

    def __init__(
        self,
        *,
        product: dict | Product,
        location: str | None = None,
        product_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        update_mask: dict | FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.product = product
        self.location = location
        self.product_id = product_id
        self.project_id = project_id
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        return hook.update_product(
            product=self.product,
            location=self.location,
            product_id=self.product_id,
            project_id=self.project_id,
            update_mask=self.update_mask,  # type: ignore
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionDeleteProductOperator(GoogleCloudBaseOperator):
    """
    Permanently delete a product and its reference images.

    Metadata of the product and all its images will be deleted right away, but
    search queries against ProductSets containing the product may still work
    until all related caches are refreshed.

    Possible errors:

    - Returns `NOT_FOUND` if the product does not exist.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionDeleteProductOperator`

    :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :param product_id: (Required) The resource id of this Product.
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_product_delete_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "product_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_product_delete_template_fields]

    def __init__(
        self,
        *,
        location: str,
        product_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.product_id = product_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_product(
            location=self.location,
            product_id=self.product_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionImageAnnotateOperator(GoogleCloudBaseOperator):
    """
    Run image detection and annotation for an image or a batch of images.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionImageAnnotateOperator`

    :param request: (Required) Annotation request for image or a batch.
        If a dict is provided, it must be of the same form as the protobuf
        message class:`google.cloud.vision_v1.types.AnnotateImageRequest`
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_annotate_image_template_fields]
    template_fields: Sequence[str] = (
        "request",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_annotate_image_template_fields]

    def __init__(
        self,
        *,
        request: dict | AnnotateImageRequest,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.request = request
        self.retry = retry
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        if not isinstance(self.request, list):
            response = hook.annotate_image(
                request=self.request, retry=self.retry, timeout=self.timeout
            )
        else:
            response = hook.batch_annotate_images(
                requests=self.request, retry=self.retry, timeout=self.timeout
            )

        return response


class CloudVisionCreateReferenceImageOperator(GoogleCloudBaseOperator):
    """
    Create and return a new ReferenceImage ID resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionCreateReferenceImageOperator`

    :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :param reference_image: (Required) The reference image to create. If an image ID is specified, it is
        ignored.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`google.cloud.vision_v1.types.ReferenceImage`
    :param reference_image_id: (Optional) A user-supplied resource id for the ReferenceImage to be added.
        If set, the server will attempt to use this value as the resource id. If it is already in use, an
        error is returned with code ALREADY_EXISTS. Must be at most 128 characters long. It cannot contain
        the character `/`.
    :param product_id: (Optional) The resource id of this Product.
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_reference_image_create_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "reference_image",
        "product_id",
        "reference_image_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_reference_image_create_template_fields]

    def __init__(
        self,
        *,
        location: str,
        reference_image: dict | ReferenceImage,
        product_id: str,
        reference_image_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.product_id = product_id
        self.reference_image = reference_image
        self.reference_image_id = reference_image_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        try:
            hook = CloudVisionHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )

            if isinstance(self.reference_image, dict):
                self.reference_image = ReferenceImage(self.reference_image)
            return hook.create_reference_image(
                location=self.location,
                product_id=self.product_id,
                reference_image=self.reference_image,
                reference_image_id=self.reference_image_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info(
                "ReferenceImage with id %s already exists. Exiting from the create operation.",
                self.product_id,
            )
            return self.reference_image_id


class CloudVisionDeleteReferenceImageOperator(GoogleCloudBaseOperator):
    """
    Delete a ReferenceImage ID resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionDeleteReferenceImageOperator`

    :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
        us-east1, us-west1, europe-west1, asia-east1
    :param reference_image_id: (Optional) A user-supplied resource id for the ReferenceImage to be added.
        If set, the server will attempt to use this value as the resource id. If it is already in use, an
        error is returned with code ALREADY_EXISTS. Must be at most 128 characters long. It cannot contain
        the character `/`.
    :param product_id: (Optional) The resource id of this Product.
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_reference_image_create_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "product_id",
        "reference_image_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_reference_image_create_template_fields]

    def __init__(
        self,
        *,
        location: str,
        product_id: str,
        reference_image_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.product_id = product_id
        self.reference_image_id = reference_image_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_reference_image(
            location=self.location,
            product_id=self.product_id,
            reference_image_id=self.reference_image_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionAddProductToProductSetOperator(GoogleCloudBaseOperator):
    """
    Add a Product to the specified ProductSet.

    If the Product is already present, no change is made. One Product can be
    added to at most 100 ProductSets.

    Possible errors:

    - Returns `NOT_FOUND` if the Product or the ProductSet doesn't exist.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionAddProductToProductSetOperator`

    :param product_set_id: (Required) The resource id for the ProductSet to modify.
    :param product_id: (Required) The resource id of this Product.
    :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
        are: us-east1, us-west1, europe-west1, asia-east1
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_add_product_to_product_set_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "product_set_id",
        "product_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_add_product_to_product_set_template_fields]

    def __init__(
        self,
        *,
        product_set_id: str,
        product_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.product_set_id = product_set_id
        self.product_id = product_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.add_product_to_product_set(
            product_set_id=self.product_set_id,
            product_id=self.product_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionRemoveProductFromProductSetOperator(GoogleCloudBaseOperator):
    """
    Remove a Product from the specified ProductSet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionRemoveProductFromProductSetOperator`

    :param product_set_id: (Required) The resource id for the ProductSet to modify.
    :param product_id: (Required) The resource id of this Product.
    :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
        are: us-east1, us-west1, europe-west1, asia-east1
    :param project_id: (Optional) The project in which the Product is located. If set to None or
        missing, the default project_id from the Google Cloud connection is used.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_remove_product_from_product_set_template_fields]
    template_fields: Sequence[str] = (
        "location",
        "product_set_id",
        "product_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_remove_product_from_product_set_template_fields]

    def __init__(
        self,
        *,
        product_set_id: str,
        product_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.product_set_id = product_set_id
        self.product_id = product_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.remove_product_from_product_set(
            product_set_id=self.product_set_id,
            product_id=self.product_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudVisionDetectTextOperator(GoogleCloudBaseOperator):
    """
    Detect Text in the image.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionDetectTextOperator`

    :param image: (Required) The image to analyze. See more:
        https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
    :param max_results: (Optional) Number of results to return.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: Number of seconds before timing out.
    :param language_hints: List of languages to use for TEXT_DETECTION.
        In most cases, an empty value yields the best results since it enables automatic language detection.
        For languages based on the Latin alphabet, setting language_hints is not needed.
    :param web_detection_params: Parameters for web detection.
    :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
        :class:`google.cloud.vision_v1.types.AnnotateImageRequest`
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_detect_text_set_template_fields]
    template_fields: Sequence[str] = (
        "image",
        "max_results",
        "timeout",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_detect_text_set_template_fields]

    def __init__(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        language_hints: str | list[str] | None = None,
        web_detection_params: dict | None = None,
        additional_properties: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.image = image
        self.max_results = max_results
        self.retry = retry
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.kwargs = kwargs
        self.additional_properties = prepare_additional_parameters(
            additional_properties=additional_properties,
            language_hints=language_hints,
            web_detection_params=web_detection_params,
        )
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.text_detection(
            image=self.image,
            max_results=self.max_results,
            retry=self.retry,
            timeout=self.timeout,
            additional_properties=self.additional_properties,
        )


class CloudVisionTextDetectOperator(GoogleCloudBaseOperator):
    """
    Detect Document Text in the image.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionTextDetectOperator`

    :param image: (Required) The image to analyze. See more:
        https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
    :param max_results: Number of results to return.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: Number of seconds before timing out.
    :param language_hints: List of languages to use for TEXT_DETECTION.
        In most cases, an empty value yields the best results since it enables automatic language detection.
        For languages based on the Latin alphabet, setting language_hints is not needed.
    :param web_detection_params: Parameters for web detection.
    :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
        https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.AnnotateImageRequest
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_document_detect_text_set_template_fields]
    template_fields: Sequence[str] = (
        "image",
        "max_results",
        "timeout",
        "gcp_conn_id",
        "impersonation_chain",
    )  # Iterable[str]
    # [END vision_document_detect_text_set_template_fields]

    def __init__(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        language_hints: str | list[str] | None = None,
        web_detection_params: dict | None = None,
        additional_properties: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.image = image
        self.max_results = max_results
        self.retry = retry
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.additional_properties = prepare_additional_parameters(
            additional_properties=additional_properties,
            language_hints=language_hints,
            web_detection_params=web_detection_params,
        )
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.document_text_detection(
            image=self.image,
            max_results=self.max_results,
            retry=self.retry,
            timeout=self.timeout,
            additional_properties=self.additional_properties,
        )


class CloudVisionDetectImageLabelsOperator(GoogleCloudBaseOperator):
    """
    Detect Document Text in the image.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionDetectImageLabelsOperator`

    :param image: (Required) The image to analyze. See more:
        https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
    :param max_results: Number of results to return.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: Number of seconds before timing out.
    :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
        https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.AnnotateImageRequest
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_detect_labels_template_fields]
    template_fields: Sequence[str] = (
        "image",
        "max_results",
        "timeout",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_detect_labels_template_fields]

    def __init__(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        additional_properties: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.image = image
        self.max_results = max_results
        self.retry = retry
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.additional_properties = additional_properties
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.label_detection(
            image=self.image,
            max_results=self.max_results,
            retry=self.retry,
            timeout=self.timeout,
            additional_properties=self.additional_properties,
        )


class CloudVisionDetectImageSafeSearchOperator(GoogleCloudBaseOperator):
    """
    Detect Document Text in the image.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudVisionDetectImageSafeSearchOperator`

    :param image: (Required) The image to analyze. See more:
        https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
    :param max_results: Number of results to return.
    :param retry: (Optional) A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param timeout: Number of seconds before timing out.
    :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
        https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.AnnotateImageRequest
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START vision_detect_safe_search_template_fields]
    template_fields: Sequence[str] = (
        "image",
        "max_results",
        "timeout",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END vision_detect_safe_search_template_fields]

    def __init__(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        additional_properties: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.image = image
        self.max_results = max_results
        self.retry = retry
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.additional_properties = additional_properties
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudVisionHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.safe_search_detection(
            image=self.image,
            max_results=self.max_results,
            retry=self.retry,
            timeout=self.timeout,
            additional_properties=self.additional_properties,
        )


def prepare_additional_parameters(
    additional_properties: dict | None, language_hints: Any, web_detection_params: Any
) -> dict | None:
    """
    Create a value for the ``additional_properties`` parameter.

    The new value is based on ``language_hints``, ``web_detection_params``, and
    ``additional_properties`` parameters specified by the user.
    """
    if language_hints is None and web_detection_params is None:
        return additional_properties

    if additional_properties is None:
        return {}

    merged_additional_parameters = deepcopy(additional_properties)

    if "image_context" not in merged_additional_parameters:
        merged_additional_parameters["image_context"] = {}

    merged_additional_parameters["image_context"]["language_hints"] = (
        merged_additional_parameters[
            "image_context"
        ].get("language_hints", language_hints)
    )
    merged_additional_parameters["image_context"]["web_detection_params"] = (
        merged_additional_parameters[
            "image_context"
        ].get("web_detection_params", web_detection_params)
    )

    return merged_additional_parameters
