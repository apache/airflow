:mod:`airflow.providers.google.cloud.operators.functions`
=========================================================

.. py:module:: airflow.providers.google.cloud.operators.functions

.. autoapi-nested-parse::

   This module contains Google Cloud Functions operators.



Module Contents
---------------

.. function:: _validate_available_memory_in_mb(value)

.. function:: _validate_max_instances(value)

.. data:: CLOUD_FUNCTION_VALIDATION
   :annotation: :List[Dict[str, Any]]

   

.. py:class:: CloudFunctionDeployFunctionOperator(*, location: str, body: Dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', zip_path: Optional[str] = None, validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a function in Google Cloud Functions.
   If a function with this name already exists, it will be updated.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudFunctionDeployFunctionOperator`

   :param location: Google Cloud region where the function should be created.
   :type location: str
   :param body: Body of the Cloud Functions definition. The body must be a
       Cloud Functions dictionary as described in:
       https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions
       . Different API versions require different variants of the Cloud Functions
       dictionary.
   :type body: dict or google.cloud.functions.v1.CloudFunction
   :param project_id: (Optional) Google Cloud project ID where the function
       should be created.
   :type project_id: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
       Default 'google_cloud_default'.
   :type gcp_conn_id: str
   :param api_version: (Optional) API version used (for example v1 - default -  or
       v1beta1).
   :type api_version: str
   :param zip_path: Path to zip file containing source code of the function. If the path
       is set, the sourceUploadUrl should not be specified in the body or it should
       be empty. Then the zip file will be uploaded using the upload URL generated
       via generateUploadUrl from the Cloud Functions API.
   :type zip_path: str
   :param validate_body: If set to False, body validation is not performed.
   :type validate_body: bool
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
      :annotation: = ['body', 'project_id', 'location', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: _validate_all_body_fields(self)



   
   .. method:: _create_new_function(self, hook)



   
   .. method:: _update_function(self, hook)



   
   .. method:: _check_if_function_exists(self, hook)



   
   .. method:: _upload_source_code(self, hook)



   
   .. method:: _set_airflow_version_label(self)



   
   .. method:: execute(self, context)




.. data:: GCF_SOURCE_ARCHIVE_URL
   :annotation: = sourceArchiveUrl

   

.. data:: GCF_SOURCE_UPLOAD_URL
   :annotation: = sourceUploadUrl

   

.. data:: SOURCE_REPOSITORY
   :annotation: = sourceRepository

   

.. data:: GCF_ZIP_PATH
   :annotation: = zip_path

   

.. py:class:: ZipPathPreprocessor(body: dict, zip_path: Optional[str] = None)

   Pre-processes zip path parameter.

   Responsible for checking if the zip path parameter is correctly specified in
   relation with source_code body fields. Non empty zip path parameter is special because
   it is mutually exclusive with sourceArchiveUrl and sourceRepository body fields.
   It is also mutually exclusive with non-empty sourceUploadUrl.
   The pre-process modifies sourceUploadUrl body field in special way when zip_path
   is not empty. An extra step is run when execute method is called and sourceUploadUrl
   field value is set in the body with the value returned by generateUploadUrl Cloud
   Function API method.

   :param body: Body passed to the create/update method calls.
   :type body: dict
   :param zip_path: (optional) Path to zip file containing source code of the function. If the path
       is set, the sourceUploadUrl should not be specified in the body or it should
       be empty. Then the zip file will be uploaded using the upload URL generated
       via generateUploadUrl from the Cloud Functions API.
   :type zip_path: str

   .. attribute:: upload_function
      :annotation: :Optional[bool]

      

   
   .. staticmethod:: _is_present_and_empty(dictionary, field)



   
   .. method:: _verify_upload_url_and_no_zip_path(self)



   
   .. method:: _verify_upload_url_and_zip_path(self)



   
   .. method:: _verify_archive_url_and_zip_path(self)



   
   .. method:: should_upload_function(self)

      Checks if function source should be uploaded.

      :rtype: bool



   
   .. method:: preprocess_body(self)

      Modifies sourceUploadUrl body field in special way when zip_path
      is not empty.




.. data:: FUNCTION_NAME_PATTERN
   :annotation: = ^projects/[^/]+/locations/[^/]+/functions/[^/]+$

   

.. data:: FUNCTION_NAME_COMPILED_PATTERN
   

   

.. py:class:: CloudFunctionDeleteFunctionOperator(*, name: str, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes the specified function from Google Cloud Functions.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudFunctionDeleteFunctionOperator`

   :param name: A fully-qualified function name, matching
       the pattern: `^projects/[^/]+/locations/[^/]+/functions/[^/]+$`
   :type name: str
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (for example v1 or v1beta1).
   :type api_version: str
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
      :annotation: = ['name', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudFunctionInvokeFunctionOperator(*, function_id: str, input_data: Dict, location: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Invokes a deployed Cloud Function. To be used for testing
   purposes as very limited traffic is allowed.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudFunctionDeployFunctionOperator`

   :param function_id: ID of the function to be called
   :type function_id: str
   :param input_data: Input to be passed to the function
   :type input_data: Dict
   :param location: The location where the function is located.
   :type location: str
   :param project_id: Optional, Google Cloud Project project_id where the function belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :return: None

   .. attribute:: template_fields
      :annotation: = ['function_id', 'input_data', 'location', 'project_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Dict)




