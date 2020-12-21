:mod:`airflow.providers.google.cloud.operators.vision`
======================================================

.. py:module:: airflow.providers.google.cloud.operators.vision

.. autoapi-nested-parse::

   This module contains a Google Cloud Vision operator.



Module Contents
---------------

.. data:: MetaData
   

   

.. py:class:: CloudVisionCreateProductSetOperator(*, product_set: Union[dict, ProductSet], location: str, project_id: Optional[str] = None, product_set_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new ProductSet resource.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionCreateProductSetOperator`

   :param product_set: (Required) The ProductSet to create. If a dict is provided, it must be of the same
       form as the protobuf message `ProductSet`.
   :type product_set: dict or google.cloud.vision_v1.types.ProductSet
   :param location: (Required) The region where the ProductSet should be created. Valid regions
       (as of 2019-02-05) are: us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param project_id: (Optional) The project in which the ProductSet should be created. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
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
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_set_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionGetProductSetOperator(*, location: str, product_set_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets information associated with a ProductSet.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionGetProductSetOperator`

   :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
       are: us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param product_set_id: (Required) The resource id of this ProductSet.
   :type product_set_id: str
   :param project_id: (Optional) The project in which the ProductSet is located. If set
       to None or missing, the default `project_id` from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_set_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionUpdateProductSetOperator(*, product_set: Union[Dict, ProductSet], location: Optional[str] = None, product_set_id: Optional[str] = None, project_id: Optional[str] = None, update_mask: Union[Dict, FieldMask] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
       :ref:`howto/operator:CloudVisionUpdateProductSetOperator`

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
       missing, the default project_id from the Google Cloud connection is used.
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
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_set_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionDeleteProductSetOperator(*, location: str, product_set_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Permanently deletes a `ProductSet`. `Products` and `ReferenceImages` in the
   `ProductSet` are not deleted. The actual image files are not deleted from Google
   Cloud Storage.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionDeleteProductSetOperator`

   :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
       are: us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param product_set_id: (Required) The resource id of this ProductSet.
   :type product_set_id: str
   :param project_id: (Optional) The project in which the ProductSet should be created.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_set_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionCreateProductOperator(*, location: str, product: str, project_id: Optional[str] = None, product_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates and returns a new product resource.

   Possible errors regarding the `Product` object provided:

   - Returns `INVALID_ARGUMENT` if `display_name` is missing or longer than 4096 characters.
   - Returns `INVALID_ARGUMENT` if `description` is longer than 4096 characters.
   - Returns `INVALID_ARGUMENT` if `product_category` is missing or invalid.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionCreateProductOperator`

   :param location: (Required) The region where the Product should be created. Valid regions
       (as of 2019-02-05) are: us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param product: (Required) The product to create. If a dict is provided, it must be of the same form as
       the protobuf message `Product`.
   :type product: dict or google.cloud.vision_v1.types.Product
   :param project_id: (Optional) The project in which the Product should be created. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
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
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionGetProductOperator(*, location: str, product_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets information associated with a `Product`.

   Possible errors:

   - Returns `NOT_FOUND` if the `Product` does not exist.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionGetProductOperator`

   :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
       us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param product_id: (Required) The resource id of this Product.
   :type product_id: str
   :param project_id: (Optional) The project in which the Product is located. If set to
       None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionUpdateProductOperator(*, product: Union[Dict, Product], location: Optional[str] = None, product_id: Optional[str] = None, project_id: Optional[str] = None, update_mask: Union[Dict, FieldMask] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :type product: dict or google.cloud.vision_v1.types.ProductSet
   :param location: (Optional) The region where the Product is located. Valid regions (as of 2019-02-05) are:
       us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param product_id: (Optional) The resource id of this Product.
   :type product_id: str
   :param project_id: (Optional) The project in which the Product is located. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
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
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionDeleteProductOperator(*, location: str, product_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Permanently deletes a product and its reference images.

   Metadata of the product and all its images will be deleted right away, but search queries against
   ProductSets containing the product may still work until all related caches are refreshed.

   Possible errors:

   - Returns `NOT_FOUND` if the product does not exist.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionDeleteProductOperator`

   :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
       us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param product_id: (Required) The resource id of this Product.
   :type product_id: str
   :param project_id: (Optional) The project in which the Product is located. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'project_id', 'product_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionImageAnnotateOperator(*, request: Union[Dict, AnnotateImageRequest], retry: Optional[Retry] = None, timeout: Optional[float] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Run image detection and annotation for an image or a batch of images.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionImageAnnotateOperator`

   :param request: (Required) Annotation request for image or a batch.
       If a dict is provided, it must be of the same form as the protobuf
       message class:`google.cloud.vision_v1.types.AnnotateImageRequest`
   :type request: list[dict or google.cloud.vision_v1.types.AnnotateImageRequest] for batch or
       dict or google.cloud.vision_v1.types.AnnotateImageRequest for single image.
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['request', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionCreateReferenceImageOperator(*, location: str, reference_image: Union[Dict, ReferenceImage], product_id: str, reference_image_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates and returns a new ReferenceImage ID resource.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionCreateReferenceImageOperator`

   :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
       us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param reference_image: (Required) The reference image to create. If an image ID is specified, it is
       ignored.
       If a dict is provided, it must be of the same form as the protobuf message
       :class:`google.cloud.vision_v1.types.ReferenceImage`
   :type reference_image: dict or google.cloud.vision_v1.types.ReferenceImage
   :param reference_image_id: (Optional) A user-supplied resource id for the ReferenceImage to be added.
       If set, the server will attempt to use this value as the resource id. If it is already in use, an
       error is returned with code ALREADY_EXISTS. Must be at most 128 characters long. It cannot contain
       the character `/`.
   :type reference_image_id: str
   :param product_id: (Optional) The resource id of this Product.
   :type product_id: str
   :param project_id: (Optional) The project in which the Product is located. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'reference_image', 'product_id', 'reference_image_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionDeleteReferenceImageOperator(*, location: str, product_id: str, reference_image_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a ReferenceImage ID resource.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionDeleteReferenceImageOperator`

   :param location: (Required) The region where the Product is located. Valid regions (as of 2019-02-05) are:
       us-east1, us-west1, europe-west1, asia-east1
   :type location: str
   :param reference_image_id: (Optional) A user-supplied resource id for the ReferenceImage to be added.
       If set, the server will attempt to use this value as the resource id. If it is already in use, an
       error is returned with code ALREADY_EXISTS. Must be at most 128 characters long. It cannot contain
       the character `/`.
   :type reference_image_id: str
   :param product_id: (Optional) The resource id of this Product.
   :type product_id: str
   :param project_id: (Optional) The project in which the Product is located. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'product_id', 'reference_image_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionAddProductToProductSetOperator(*, product_set_id: str, product_id: str, location: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Adds a Product to the specified ProductSet. If the Product is already present, no change is made.

   One Product can be added to at most 100 ProductSets.

   Possible errors:

   - Returns `NOT_FOUND` if the Product or the ProductSet doesn’t exist.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionAddProductToProductSetOperator`

   :param product_set_id: (Required) The resource id for the ProductSet to modify.
   :type product_set_id: str
   :param product_id: (Required) The resource id of this Product.
   :type product_id: str
   :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
       are: us-east1, us-west1, europe-west1, asia-east1
   :type: str
   :param project_id: (Optional) The project in which the Product is located. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'product_set_id', 'product_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionRemoveProductFromProductSetOperator(*, product_set_id: str, product_id: str, location: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Removes a Product from the specified ProductSet.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionRemoveProductFromProductSetOperator`

   :param product_set_id: (Required) The resource id for the ProductSet to modify.
   :type product_set_id: str
   :param product_id: (Required) The resource id of this Product.
   :type product_id: str
   :param location: (Required) The region where the ProductSet is located. Valid regions (as of 2019-02-05)
       are: us-east1, us-west1, europe-west1, asia-east1
   :type: str
   :param project_id: (Optional) The project in which the Product is located. If set to None or
       missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to
       complete. Note that if retry is specified, the timeout applies to each individual
       attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['location', 'product_set_id', 'product_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionDetectTextOperator(image: Union[Dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, language_hints: Optional[Union[str, List[str]]] = None, web_detection_params: Optional[Dict] = None, additional_properties: Optional[Dict] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Detects Text in the image

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionDetectTextOperator`

   :param image: (Required) The image to analyze. See more:
       https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
   :type image: dict or google.cloud.vision_v1.types.Image
   :param max_results: (Optional) Number of results to return.
   :type max_results: int
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: Number of seconds before timing out.
   :type timeout: float
   :param language_hints: List of languages to use for TEXT_DETECTION.
       In most cases, an empty value yields the best results since it enables automatic language detection.
       For languages based on the Latin alphabet, setting language_hints is not needed.
   :type language_hints: str or list[str]
   :param web_detection_params: Parameters for web detection.
   :type web_detection_params: dict
   :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
       :class:`google.cloud.vision_v1.types.AnnotateImageRequest`
   :type additional_properties: dict
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['image', 'max_results', 'timeout', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionTextDetectOperator(image: Union[Dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, language_hints: Optional[Union[str, List[str]]] = None, web_detection_params: Optional[Dict] = None, additional_properties: Optional[Dict] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Detects Document Text in the image

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionTextDetectOperator`

   :param image: (Required) The image to analyze. See more:
       https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
   :type image: dict or google.cloud.vision_v1.types.Image
   :param max_results: Number of results to return.
   :type max_results: int
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: Number of seconds before timing out.
   :type timeout: float
   :param language_hints: List of languages to use for TEXT_DETECTION.
       In most cases, an empty value yields the best results since it enables automatic language detection.
       For languages based on the Latin alphabet, setting language_hints is not needed.
   :type language_hints: str or list[str]
   :param web_detection_params: Parameters for web detection.
   :type web_detection_params: dict
   :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
       https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.AnnotateImageRequest
   :type additional_properties: dict
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['image', 'max_results', 'timeout', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionDetectImageLabelsOperator(image: Union[Dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, additional_properties: Optional[Dict] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Detects Document Text in the image

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionDetectImageLabelsOperator`

   :param image: (Required) The image to analyze. See more:
       https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
   :type image: dict or google.cloud.vision_v1.types.Image
   :param max_results: Number of results to return.
   :type max_results: int
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: Number of seconds before timing out.
   :type timeout: float
   :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
       https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.AnnotateImageRequest
   :type additional_properties: dict
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['image', 'max_results', 'timeout', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVisionDetectImageSafeSearchOperator(image: Union[Dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, additional_properties: Optional[Dict] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Detects Document Text in the image

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVisionDetectImageSafeSearchOperator`

   :param image: (Required) The image to analyze. See more:
       https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.Image
   :type image: dict or google.cloud.vision_v1.types.Image
   :param max_results: Number of results to return.
   :type max_results: int
   :param retry: (Optional) A retry object used to retry requests. If `None` is
       specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: Number of seconds before timing out.
   :type timeout: float
   :param additional_properties: Additional properties to be set on the AnnotateImageRequest. See more:
       https://googleapis.github.io/google-cloud-python/latest/vision/gapic/v1/types.html#google.cloud.vision_v1.types.AnnotateImageRequest
   :type additional_properties: dict
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['image', 'max_results', 'timeout', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. function:: prepare_additional_parameters(additional_properties: Optional[Dict], language_hints: Any, web_detection_params: Any) -> Optional[Dict]
   Creates additional_properties parameter based on language_hints, web_detection_params and
   additional_properties parameters specified by the user


