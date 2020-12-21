:mod:`airflow.providers.google.marketing_platform.hooks.display_video`
======================================================================

.. py:module:: airflow.providers.google.marketing_platform.hooks.display_video

.. autoapi-nested-parse::

   This module contains Google DisplayVideo hook.



Module Contents
---------------

.. py:class:: GoogleDisplayVideo360Hook(api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Display & Video 360.

   .. attribute:: _conn
      :annotation: :Optional[Any]

      

   
   .. method:: get_conn(self)

      Retrieves connection to DisplayVideo.



   
   .. method:: get_conn_to_display_video(self)

      Retrieves connection to DisplayVideo.



   
   .. staticmethod:: erf_uri(partner_id, entity_type)

      Return URI for all Entity Read Files in bucket.

      For example, if you were generating a file name to retrieve the entity read file
      for partner 123 accessing the line_item table from April 2, 2013, your filename
      would look something like this:
      gdbm-123/entity/20130402.0.LineItem.json

      More information:
      https://developers.google.com/bid-manager/guides/entity-read/overview

      :param partner_id The numeric ID of your Partner.
      :type partner_id: int
      :param entity_type: The type of file Partner, Advertiser, InsertionOrder,
      LineItem, Creative, Pixel, InventorySource, UserList, UniversalChannel, and summary.
      :type entity_type: str



   
   .. method:: create_query(self, query: Dict[str, Any])

      Creates a query.

      :param query: Query object to be passed to request body.
      :type query: Dict[str, Any]



   
   .. method:: delete_query(self, query_id: str)

      Deletes a stored query as well as the associated stored reports.

      :param query_id: Query ID to delete.
      :type query_id: str



   
   .. method:: get_query(self, query_id: str)

      Retrieves a stored query.

      :param query_id: Query ID to retrieve.
      :type query_id: str



   
   .. method:: list_queries(self)

      Retrieves stored queries.



   
   .. method:: run_query(self, query_id: str, params: Dict[str, Any])

      Runs a stored query to generate a report.

      :param query_id: Query ID to run.
      :type query_id: str
      :param params: Parameters for the report.
      :type params: Dict[str, Any]



   
   .. method:: upload_line_items(self, line_items: Any)

      Uploads line items in CSV format.

      :param line_items: downloaded data from GCS and passed to the body request
      :type line_items: Any
      :return: response body.
      :rtype: List[Dict[str, Any]]



   
   .. method:: download_line_items(self, request_body: Dict[str, Any])

      Retrieves line items in CSV format.

      :param request_body: dictionary with parameters that should be passed into.
          More information about it can be found here:
          https://developers.google.com/bid-manager/v1.1/lineitems/downloadlineitems
      :type request_body: Dict[str, Any]



   
   .. method:: create_sdf_download_operation(self, body_request: Dict[str, Any])

      Creates an SDF Download Task and Returns an Operation.

      :param body_request: Body request.
      :type body_request: Dict[str, Any]

      More information about body request n be found here:
      https://developers.google.com/display-video/api/reference/rest/v1/sdfdownloadtasks/create



   
   .. method:: get_sdf_download_operation(self, operation_name: str)

      Gets the latest state of an asynchronous SDF download task operation.

      :param operation_name: The name of the operation resource.
      :type operation_name: str



   
   .. method:: download_media(self, resource_name: str)

      Downloads media.

      :param resource_name: of the media that is being downloaded.
      :type resource_name: str




