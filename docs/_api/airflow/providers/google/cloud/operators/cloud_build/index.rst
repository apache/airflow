:mod:`airflow.providers.google.cloud.operators.cloud_build`
===========================================================

.. py:module:: airflow.providers.google.cloud.operators.cloud_build

.. autoapi-nested-parse::

   Operators that integrate with Google Cloud Build service.



Module Contents
---------------

.. data:: REGEX_REPO_PATH
   

   

.. py:class:: BuildProcessor(body: dict)

   Processes build configurations to add additional functionality to support the use of operators.

   The following improvements are made:

   * It is required to provide the source and only one type can be given,
   * It is possible to provide the source as the URL address instead dict.

   :param body: The request body.
       See: https://cloud.google.com/cloud-build/docs/api/reference/rest/v1/projects.builds
   :type body: dict

   
   .. method:: _verify_source(self)



   
   .. method:: _reformat_source(self)



   
   .. method:: _reformat_repo_source(self)



   
   .. method:: _reformat_storage_source(self)



   
   .. method:: process_body(self)

      Processes the body passed in the constructor

      :return: the body.
      :type: dict



   
   .. staticmethod:: _convert_repo_url_to_dict(source)

      Convert url to repository in Google Cloud Source to a format supported by the API

      Example valid input:

      .. code-block:: none

          https://source.developers.google.com/p/airflow-project/r/airflow-repo#branch-name



   
   .. staticmethod:: _convert_storage_url_to_dict(storage_url: str)

      Convert url to object in Google Cloud Storage to a format supported by the API

      Example valid input:

      .. code-block:: none

          gs://bucket-name/object-name.tar.gz




.. py:class:: CloudBuildCreateBuildOperator(*, body: Union[dict, str], project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Starts a build with the specified configuration.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudBuildCreateBuildOperator`

   :param body: The build config with instructions to perform with CloudBuild.
       Can be a dictionary or path to a file type like YAML or JSON.
       See: https://cloud.google.com/cloud-build/docs/api/reference/rest/v1/projects.builds
   :type body: dict or string
   :param project_id: ID of the Google Cloud project if None then
       default project_id is used.
   :type project_id: str
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
      :annotation: = ['body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.yml', '.yaml', '.json']

      

   
   .. method:: prepare_template(self)



   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




