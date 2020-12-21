:mod:`airflow.providers.google.cloud.hooks.vision`
==================================================

.. py:module:: airflow.providers.google.cloud.hooks.vision

.. autoapi-nested-parse::

   This module contains a Google Cloud Vision Hook.



Module Contents
---------------

.. data:: ERR_DIFF_NAMES
   :annotation: = The {label} name provided in the object ({explicit_name}) is different
    than the name created from the input parameters ({constructed_name}). Please either:
    1) Remove the {label} name,
    2) Remove the location and {id_label} parameters,
    3) Unify the {label} name and input parameters.
    

   

.. data:: ERR_UNABLE_TO_CREATE
   :annotation: = Unable to determine the {label} name. Please either set the name directly
    in the {label} object or provide the `location` and `{id_label}` parameters.
    

   

.. py:class:: NameDeterminer(label: str, id_label: str, get_path: Callable[[str, str, str], str])

   Helper class to determine entity name.

   
   .. method:: get_entity_with_name(self, entity: Any, entity_id: Optional[str], location: Optional[str], project_id: str)

      Check if entity has the `name` attribute set:
      * If so, no action is taken.

      * If not, and the name can be constructed from other parameters provided, it is created and filled in
          the entity.

      * If both the entity's 'name' attribute is set and the name can be constructed from other parameters
          provided:

          * If they are the same - no action is taken

          * if they are different - an exception is thrown.


      :param entity: Entity
      :type entity: any
      :param entity_id: Entity id
      :type entity_id: str
      :param location: Location
      :type location: str
      :param project_id: The id of Google Cloud Vision project.
      :type project_id: str
      :return: The same entity or entity with new name
      :rtype: str
      :raises: AirflowException




.. py:class:: CloudVisionHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Vision APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   .. attribute:: product_name_determiner
      

      

   .. attribute:: product_set_name_determiner
      

      

   
   .. method:: get_conn(self)

      Retrieves connection to Cloud Vision.

      :return: Google Cloud Vision client object.
      :rtype: google.cloud.vision_v1.ProductSearchClient



   
   .. method:: annotator_client(self)

      Creates ImageAnnotatorClient.

      :return: Google Image Annotator client object.
      :rtype: google.cloud.vision_v1.ImageAnnotatorClient



   
   .. staticmethod:: _check_for_error(response: Dict)



   
   .. method:: create_product_set(self, location: str, product_set: Union[dict, ProductSet], project_id: str, product_set_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator`



   
   .. method:: get_product_set(self, location: str, product_set_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator`



   
   .. method:: update_product_set(self, product_set: Union[dict, ProductSet], project_id: str, location: Optional[str] = None, product_set_id: Optional[str] = None, update_mask: Union[dict, FieldMask] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator`



   
   .. method:: delete_product_set(self, location: str, product_set_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator`



   
   .. method:: create_product(self, location: str, product: Union[dict, Product], project_id: str, product_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator`



   
   .. method:: get_product(self, location: str, product_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator`



   
   .. method:: update_product(self, product: Union[dict, Product], project_id: str, location: Optional[str] = None, product_id: Optional[str] = None, update_mask: Optional[Dict[str, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator`



   
   .. method:: delete_product(self, location: str, product_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator`



   
   .. method:: create_reference_image(self, location: str, product_id: str, reference_image: Union[dict, ReferenceImage], project_id: str, reference_image_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator`



   
   .. method:: delete_reference_image(self, location: str, product_id: str, reference_image_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDeleteReferenceImageOperator`



   
   .. method:: add_product_to_product_set(self, product_set_id: str, product_id: str, project_id: str, location: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator`



   
   .. method:: remove_product_from_product_set(self, product_set_id: str, product_id: str, project_id: str, location: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator` # pylint: disable=line-too-long # noqa



   
   .. method:: annotate_image(self, request: Union[dict, AnnotateImageRequest], retry: Optional[Retry] = None, timeout: Optional[float] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator`



   
   .. method:: batch_annotate_images(self, requests: Union[List[dict], List[AnnotateImageRequest]], retry: Optional[Retry] = None, timeout: Optional[float] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator`



   
   .. method:: text_detection(self, image: Union[dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, additional_properties: Optional[Dict] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator`



   
   .. method:: document_text_detection(self, image: Union[dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, additional_properties: Optional[dict] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator`



   
   .. method:: label_detection(self, image: Union[dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, additional_properties: Optional[dict] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator`



   
   .. method:: safe_search_detection(self, image: Union[dict, Image], max_results: Optional[int] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, additional_properties: Optional[dict] = None)

      For the documentation see:
      :py:class:`~airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator`



   
   .. staticmethod:: _get_autogenerated_id(response)




