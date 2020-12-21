:mod:`airflow.providers.amazon.aws.transfers.google_api_to_s3`
==============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.google_api_to_s3

.. autoapi-nested-parse::

   This module allows you to transfer data from any Google API endpoint into a S3 Bucket.



Module Contents
---------------

.. py:class:: GoogleApiToS3Operator(*, google_api_service_name: str, google_api_service_version: str, google_api_endpoint_path: str, google_api_endpoint_params: dict, s3_destination_key: str, google_api_response_via_xcom: Optional[str] = None, google_api_endpoint_params_via_xcom: Optional[str] = None, google_api_endpoint_params_via_xcom_task_ids: Optional[str] = None, google_api_pagination: bool = False, google_api_num_retries: int = 0, s3_overwrite: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, aws_conn_id: str = 'aws_default', google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Basic class for transferring data from a Google API endpoint into a S3 Bucket.

   This discovery-based operator use
   :class:`~airflow.providers.google.common.hooks.discovery_api.GoogleDiscoveryApiHook` to communicate
   with Google Services via the
   `Google API Python Client <https://github.com/googleapis/google-api-python-client>`__.
   Please note that this library is in maintenance mode hence it won't fully support Google Cloud in
   the future.
   Therefore it is recommended that you use the custom Google Cloud Service Operators for working
   with the Google Cloud Platform.

   :param google_api_service_name: The specific API service that is being requested.
   :type google_api_service_name: str
   :param google_api_service_version: The version of the API that is being requested.
   :type google_api_service_version: str
   :param google_api_endpoint_path: The client libraries path to the api call's executing method.
       For example: 'analyticsreporting.reports.batchGet'

       .. note:: See https://developers.google.com/apis-explorer
           for more information on which methods are available.

   :type google_api_endpoint_path: str
   :param google_api_endpoint_params: The params to control the corresponding endpoint result.
   :type google_api_endpoint_params: dict
   :param s3_destination_key: The url where to put the data retrieved from the endpoint in S3.
   :type s3_destination_key: str
   :param google_api_response_via_xcom: Can be set to expose the google api response to xcom.
   :type google_api_response_via_xcom: str
   :param google_api_endpoint_params_via_xcom: If set to a value this value will be used as a key
       for pulling from xcom and updating the google api endpoint params.
   :type google_api_endpoint_params_via_xcom: str
   :param google_api_endpoint_params_via_xcom_task_ids: Task ids to filter xcom by.
   :type google_api_endpoint_params_via_xcom_task_ids: str or list of str
   :param google_api_pagination: If set to True Pagination will be enabled for this request
       to retrieve all data.

       .. note:: This means the response will be a list of responses.

   :type google_api_pagination: bool
   :param google_api_num_retries: Define the number of retries for the google api requests being made
       if it fails.
   :type google_api_num_retries: int
   :param s3_overwrite: Specifies whether the s3 file will be overwritten if exists.
   :type s3_overwrite: bool
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param aws_conn_id: The connection id specifying the authentication information for the S3 Bucket.
   :type aws_conn_id: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['google_api_endpoint_params', 's3_destination_key', 'google_impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #cc181e

      

   
   .. method:: execute(self, context)

      Transfers Google APIs json data to S3.

      :param context: The context that is being provided when executing.
      :type context: dict



   
   .. method:: _retrieve_data_from_google_api(self)



   
   .. method:: _load_data_to_s3(self, data: dict)



   
   .. method:: _update_google_api_endpoint_params_via_xcom(self, task_instance: TaskInstance)



   
   .. method:: _expose_google_api_response_via_xcom(self, task_instance: TaskInstance, data: dict)




