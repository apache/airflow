:mod:`airflow.providers.google.cloud.hooks.functions`
=====================================================

.. py:module:: airflow.providers.google.cloud.hooks.functions

.. autoapi-nested-parse::

   This module contains a Google Cloud Functions Hook.



Module Contents
---------------

.. data:: TIME_TO_SLEEP_IN_SECONDS
   :annotation: = 1

   

.. py:class:: CloudFunctionsHook(api_version: str, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for the Google Cloud Functions APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   .. attribute:: _conn
      :annotation: :Optional[Any]

      

   
   .. staticmethod:: _full_location(project_id: str, location: str)

      Retrieve full location of the function in the form of
      ``projects/<GCP_PROJECT_ID>/locations/<GCP_LOCATION>``

      :param project_id: The Google Cloud Project project_id where the function belongs.
      :type project_id: str
      :param location: The location where the function is created.
      :type location: str
      :return:



   
   .. method:: get_conn(self)

      Retrieves the connection to Cloud Functions.

      :return: Google Cloud Function services object.
      :rtype: dict



   
   .. method:: get_function(self, name: str)

      Returns the Cloud Function with the given name.

      :param name: Name of the function.
      :type name: str
      :return: A Cloud Functions object representing the function.
      :rtype: dict



   
   .. method:: create_new_function(self, location: str, body: dict, project_id: str)

      Creates a new function in Cloud Function in the location specified in the body.

      :param location: The location of the function.
      :type location: str
      :param body: The body required by the Cloud Functions insert API.
      :type body: dict
      :param project_id: Optional, Google Cloud Project project_id where the function belongs.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: update_function(self, name: str, body: dict, update_mask: List[str])

      Updates Cloud Functions according to the specified update mask.

      :param name: The name of the function.
      :type name: str
      :param body: The body required by the cloud function patch API.
      :type body: dict
      :param update_mask: The update mask - array of fields that should be patched.
      :type update_mask: [str]
      :return: None



   
   .. method:: upload_function_zip(self, location: str, zip_path: str, project_id: str)

      Uploads zip file with sources.

      :param location: The location where the function is created.
      :type location: str
      :param zip_path: The path of the valid .zip file to upload.
      :type zip_path: str
      :param project_id: Optional, Google Cloud Project project_id where the function belongs.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: The upload URL that was returned by generateUploadUrl method.
      :rtype: str



   
   .. method:: delete_function(self, name: str)

      Deletes the specified Cloud Function.

      :param name: The name of the function.
      :type name: str
      :return: None



   
   .. method:: call_function(self, function_id: str, input_data: Dict, location: str, project_id: str)

      Synchronously invokes a deployed Cloud Function. To be used for testing
      purposes as very limited traffic is allowed.

      :param function_id: ID of the function to be called
      :type function_id: str
      :param input_data: Input to be passed to the function
      :type input_data: Dict
      :param location: The location where the function is located.
      :type location: str
      :param project_id: Optional, Google Cloud Project project_id where the function belongs.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: _wait_for_operation_to_complete(self, operation_name: str)

      Waits for the named operation to complete - checks status of the
      asynchronous call.

      :param operation_name: The name of the operation.
      :type operation_name: str
      :return: The response returned by the operation.
      :rtype: dict
      :exception: AirflowException in case error is returned.




