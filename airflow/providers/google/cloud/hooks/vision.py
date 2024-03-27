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
"""This module contains a Google Cloud Vision Hook."""

from __future__ import annotations

from copy import deepcopy
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.vision_v1 import (
    AnnotateImageRequest,
    Image,
    ImageAnnotatorClient,
    Product,
    ProductSearchClient,
    ProductSet,
    ReferenceImage,
)
from google.protobuf.json_format import MessageToDict

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.protobuf import field_mask_pb2

ERR_DIFF_NAMES = """The {label} name provided in the object ({explicit_name}) is different
    than the name created from the input parameters ({constructed_name}). Please either:
    1) Remove the {label} name,
    2) Remove the location and {id_label} parameters,
    3) Unify the {label} name and input parameters.
    """

ERR_UNABLE_TO_CREATE = """Unable to determine the {label} name. Please either set the name directly
    in the {label} object or provide the `location` and `{id_label}` parameters.
    """


class NameDeterminer:
    """Helper class to determine entity name."""

    def __init__(self, label: str, id_label: str, get_path: Callable[[str, str, str], str]) -> None:
        self.label = label
        self.id_label = id_label
        self.get_path = get_path

    def get_entity_with_name(
        self, entity: Any, entity_id: str | None, location: str | None, project_id: str
    ) -> Any:
        """
        Check if entity has the `name` attribute set.

        * If so, no action is taken.
        * If not, and the name can be constructed from other parameters provided, it is created and filled in
            the entity.
        * If both the entity's 'name' attribute is set and the name can be constructed from other parameters
            provided:
            * If they are the same - no action is taken
            * if they are different - an exception is thrown.

        :param entity: Entity
        :param entity_id: Entity id
        :param location: Location
        :param project_id: The id of Google Cloud Vision project.
        :return: The same entity or entity with new name
        :raises: AirflowException
        """
        entity = deepcopy(entity)
        explicit_name = getattr(entity, "name")
        if location and entity_id:
            # Necessary parameters to construct the name are present. Checking for conflict with explicit name
            constructed_name = self.get_path(project_id, location, entity_id)
            if not explicit_name:
                entity.name = constructed_name
                return entity

            if explicit_name != constructed_name:
                raise AirflowException(
                    ERR_DIFF_NAMES.format(
                        label=self.label,
                        explicit_name=explicit_name,
                        constructed_name=constructed_name,
                        id_label=self.id_label,
                    )
                )

        # Not enough parameters to construct the name. Trying to use the name from Product / ProductSet.
        if explicit_name:
            return entity
        else:
            raise AirflowException(ERR_UNABLE_TO_CREATE.format(label=self.label, id_label=self.id_label))


class CloudVisionHook(GoogleBaseHook):
    """
    Hook for Google Cloud Vision APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    _client: ProductSearchClient | None
    product_name_determiner = NameDeterminer("Product", "product_id", ProductSearchClient.product_path)
    product_set_name_determiner = NameDeterminer(
        "ProductSet", "productset_id", ProductSearchClient.product_set_path
    )

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._client = None

    def get_conn(self) -> ProductSearchClient:
        """
        Retrieve a connection to Cloud Vision.

        :return: Google Cloud Vision client object.
        """
        if not self._client:
            self._client = ProductSearchClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    @cached_property
    def annotator_client(self) -> ImageAnnotatorClient:
        """
        Creates ImageAnnotatorClient.

        :return: Google Image Annotator client object.
        """
        return ImageAnnotatorClient(credentials=self.get_credentials())

    @staticmethod
    def _check_for_error(response: dict) -> None:
        if "error" in response:
            raise AirflowException(response)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_product_set(
        self,
        location: str,
        product_set: ProductSet | None,
        project_id: str = PROVIDE_PROJECT_ID,
        product_set_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> str:
        """
        Create product set.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator`.
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}"
        self.log.info("Creating a new ProductSet under the parent: %s", parent)
        response = client.create_product_set(
            parent=parent,
            product_set=product_set,
            product_set_id=product_set_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("ProductSet created: %s", response.name if response else "")
        self.log.debug("ProductSet created:\n%s", response)

        if not product_set_id:
            # Product set id was generated by the API
            product_set_id = self._get_autogenerated_id(response)
            self.log.info("Extracted autogenerated ProductSet ID from the response: %s", product_set_id)

        return product_set_id

    @GoogleBaseHook.fallback_to_default_project_id
    def get_product_set(
        self,
        location: str,
        product_set_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> dict:
        """
        Get product set.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator`.
        """
        client = self.get_conn()
        name = ProductSearchClient.product_set_path(project_id, location, product_set_id)
        self.log.info("Retrieving ProductSet: %s", name)
        response = client.get_product_set(name=name, retry=retry, timeout=timeout, metadata=metadata)
        self.log.info("ProductSet retrieved.")
        self.log.debug("ProductSet retrieved:\n%s", response)
        return MessageToDict(response._pb)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_product_set(
        self,
        product_set: dict | ProductSet,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        product_set_id: str | None = None,
        update_mask: dict | field_mask_pb2.FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> dict:
        """
        Update product set.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator`.
        """
        client = self.get_conn()

        product_set = self.product_set_name_determiner.get_entity_with_name(
            product_set, product_set_id, location, project_id
        )
        if isinstance(product_set, dict):
            product_set = ProductSet(product_set)
        self.log.info("Updating ProductSet: %s", product_set.name)
        response = client.update_product_set(
            product_set=product_set,
            update_mask=update_mask,  # type: ignore
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("ProductSet updated: %s", response.name if response else "")
        self.log.debug("ProductSet updated:\n%s", response)
        return MessageToDict(response._pb)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_product_set(
        self,
        location: str,
        product_set_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete product set.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator`.
        """
        client = self.get_conn()
        name = ProductSearchClient.product_set_path(project_id, location, product_set_id)
        self.log.info("Deleting ProductSet: %s", name)
        client.delete_product_set(name=name, retry=retry, timeout=timeout, metadata=metadata)
        self.log.info("ProductSet with the name [%s] deleted.", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_product(
        self,
        location: str,
        product: dict | Product,
        project_id: str = PROVIDE_PROJECT_ID,
        product_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Create product.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator`.
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}"
        self.log.info("Creating a new Product under the parent: %s", parent)

        if isinstance(product, dict):
            product = Product(product)
        response = client.create_product(
            parent=parent,
            product=product,
            product_id=product_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Product created: %s", response.name if response else "")
        self.log.debug("Product created:\n%s", response)

        if not product_id:
            # Product id was generated by the API
            product_id = self._get_autogenerated_id(response)
            self.log.info("Extracted autogenerated Product ID from the response: %s", product_id)

        return product_id

    @GoogleBaseHook.fallback_to_default_project_id
    def get_product(
        self,
        location: str,
        product_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Get product.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator`.
        """
        client = self.get_conn()
        name = ProductSearchClient.product_path(project_id, location, product_id)
        self.log.info("Retrieving Product: %s", name)
        response = client.get_product(name=name, retry=retry, timeout=timeout, metadata=metadata)
        self.log.info("Product retrieved.")
        self.log.debug("Product retrieved:\n%s", response)
        return MessageToDict(response._pb)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_product(
        self,
        product: dict | Product,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        product_id: str | None = None,
        update_mask: dict | field_mask_pb2.FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Update product.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator`.
        """
        client = self.get_conn()

        product = self.product_name_determiner.get_entity_with_name(product, product_id, location, project_id)
        if isinstance(product, dict):
            product = Product(product)
        self.log.info("Updating ProductSet: %s", product.name)
        response = client.update_product(
            product=product,
            update_mask=update_mask,  # type: ignore
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Product updated: %s", response.name if response else "")
        self.log.debug("Product updated:\n%s", response)
        return MessageToDict(response._pb)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_product(
        self,
        location: str,
        product_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete product.

        For the documentation see:
        :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator`.
        """
        client = self.get_conn()
        name = ProductSearchClient.product_path(project_id, location, product_id)
        self.log.info("Deleting ProductSet: %s", name)
        client.delete_product(name=name, retry=retry, timeout=timeout, metadata=metadata)
        self.log.info("Product with the name [%s] deleted:", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_reference_image(
        self,
        location: str,
        product_id: str,
        reference_image: dict | ReferenceImage,
        project_id: str,
        reference_image_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> str:
        """
        Create reference image.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator`.
        """
        client = self.get_conn()
        self.log.info("Creating ReferenceImage")
        parent = ProductSearchClient.product_path(project=project_id, location=location, product=product_id)

        if isinstance(reference_image, dict):
            reference_image = ReferenceImage(reference_image)
        response = client.create_reference_image(
            parent=parent,
            reference_image=reference_image,
            reference_image_id=reference_image_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("ReferenceImage created: %s", response.name if response else "")
        self.log.debug("ReferenceImage created:\n%s", response)

        if not reference_image_id:
            # Reference image  id was generated by the API
            reference_image_id = self._get_autogenerated_id(response)
            self.log.info(
                "Extracted autogenerated ReferenceImage ID from the response: %s", reference_image_id
            )

        return reference_image_id

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_reference_image(
        self,
        location: str,
        product_id: str,
        reference_image_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete reference image.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDeleteReferenceImageOperator`.
        """
        client = self.get_conn()
        self.log.info("Deleting ReferenceImage")
        name = ProductSearchClient.reference_image_path(
            project=project_id, location=location, product=product_id, reference_image=reference_image_id
        )

        client.delete_reference_image(
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("ReferenceImage with the name [%s] deleted.", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def add_product_to_product_set(
        self,
        product_set_id: str,
        product_id: str,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Add product to product set.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator`.
        """
        client = self.get_conn()

        product_name = ProductSearchClient.product_path(project_id, location, product_id)
        product_set_name = ProductSearchClient.product_set_path(project_id, location, product_set_id)

        self.log.info("Add Product[name=%s] to Product Set[name=%s]", product_name, product_set_name)

        client.add_product_to_product_set(
            name=product_set_name, product=product_name, retry=retry, timeout=timeout, metadata=metadata
        )

        self.log.info("Product added to Product Set")

    @GoogleBaseHook.fallback_to_default_project_id
    def remove_product_from_product_set(
        self,
        product_set_id: str,
        product_id: str,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Remove product from product set.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator`.
        """
        client = self.get_conn()

        product_name = ProductSearchClient.product_path(project_id, location, product_id)
        product_set_name = ProductSearchClient.product_set_path(project_id, location, product_set_id)

        self.log.info("Remove Product[name=%s] from Product Set[name=%s]", product_name, product_set_name)

        client.remove_product_from_product_set(
            name=product_set_name, product=product_name, retry=retry, timeout=timeout, metadata=metadata
        )

        self.log.info("Product removed from Product Set")

    def annotate_image(
        self,
        request: dict | AnnotateImageRequest,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> dict:
        """
        Annotate image.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator`.
        """
        client = self.annotator_client

        self.log.info("Annotating image")

        response = client.annotate_image(request=request, retry=retry, timeout=timeout)

        self.log.info("Image annotated")

        return MessageToDict(response._pb)

    @GoogleBaseHook.quota_retry()
    def batch_annotate_images(
        self,
        requests: list[dict] | list[AnnotateImageRequest],
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> dict:
        """
        Batch annotate images.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator`.
        """
        client = self.annotator_client

        self.log.info("Annotating images")

        requests = list(map(AnnotateImageRequest, requests))
        response = client.batch_annotate_images(requests=requests, retry=retry, timeout=timeout)

        self.log.info("Images annotated")

        return MessageToDict(response._pb)

    @GoogleBaseHook.quota_retry()
    def text_detection(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        additional_properties: dict | None = None,
    ) -> dict:
        """
        Text detection.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator`.
        """
        client = self.annotator_client

        self.log.info("Detecting text")

        if additional_properties is None:
            additional_properties = {}

        response = client.text_detection(
            image=image, max_results=max_results, retry=retry, timeout=timeout, **additional_properties
        )
        response = MessageToDict(response._pb)
        self._check_for_error(response)

        self.log.info("Text detection finished")

        return response

    @GoogleBaseHook.quota_retry()
    def document_text_detection(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        additional_properties: dict | None = None,
    ) -> dict:
        """
        Document text detection.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator`.
        """
        client = self.annotator_client

        self.log.info("Detecting document text")

        if additional_properties is None:
            additional_properties = {}

        response = client.document_text_detection(
            image=image, max_results=max_results, retry=retry, timeout=timeout, **additional_properties
        )
        response = MessageToDict(response._pb)
        self._check_for_error(response)

        self.log.info("Document text detection finished")

        return response

    @GoogleBaseHook.quota_retry()
    def label_detection(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        additional_properties: dict | None = None,
    ) -> dict:
        """
        Label detection.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator`.
        """
        client = self.annotator_client

        self.log.info("Detecting labels")

        if additional_properties is None:
            additional_properties = {}

        response = client.label_detection(
            image=image, max_results=max_results, retry=retry, timeout=timeout, **additional_properties
        )
        response = MessageToDict(response._pb)
        self._check_for_error(response)

        self.log.info("Labels detection finished")

        return response

    @GoogleBaseHook.quota_retry()
    def safe_search_detection(
        self,
        image: dict | Image,
        max_results: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        additional_properties: dict | None = None,
    ) -> dict:
        """
        Safe search detection.

        For the documentation see:
        :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator`.
        """
        client = self.annotator_client

        self.log.info("Detecting safe search")

        if additional_properties is None:
            additional_properties = {}

        response = client.safe_search_detection(
            image=image, max_results=max_results, retry=retry, timeout=timeout, **additional_properties
        )
        response = MessageToDict(response._pb)
        self._check_for_error(response)

        self.log.info("Safe search detection finished")
        return response

    @staticmethod
    def _get_autogenerated_id(response) -> str:
        try:
            name = response.name
        except AttributeError as e:
            raise AirflowException(f"Unable to get name from response... [{response}]\n{e}")
        if "/" not in name:
            raise AirflowException(f"Unable to get id from name... [{name}]")
        return name.rsplit("/", 1)[1]
