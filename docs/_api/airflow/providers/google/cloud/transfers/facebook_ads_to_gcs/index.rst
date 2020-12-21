:mod:`airflow.providers.google.cloud.transfers.facebook_ads_to_gcs`
===================================================================

.. py:module:: airflow.providers.google.cloud.transfers.facebook_ads_to_gcs

.. autoapi-nested-parse::

   This module contains Facebook Ad Reporting to GCS operators.



Module Contents
---------------

.. py:class:: FacebookAdsReportToGcsOperator(*, bucket_name: str, object_name: str, fields: List[str], params: Dict[str, Any], gzip: bool = False, api_version: str = 'v6.0', gcp_conn_id: str = 'google_cloud_default', facebook_conn_id: str = 'facebook_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Fetches the results from the Facebook Ads API as desired in the params
   Converts and saves the data as a temporary JSON file
   Uploads the JSON to Google Cloud Storage

   .. seealso::
       For more information on the Facebook Ads API, take a look at the API docs:
       https://developers.facebook.com/docs/marketing-apis/

   .. seealso::
       For more information on the Facebook Ads Python SDK, take a look at the docs:
       https://github.com/facebook/facebook-python-business-sdk

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:FacebookAdsReportToGcsOperator`

   :param bucket: The GCS bucket to upload to
   :type bucket: str
   :param obj: GCS path to save the object. Must be the full file path (ex. `path/to/file.txt`)
   :type obj: str
   :param gcp_conn_id: Airflow Google Cloud connection ID
   :type gcp_conn_id: str
   :param facebook_conn_id: Airflow Facebook Ads connection ID
   :type facebook_conn_id: str
   :param api_version: The version of Facebook API. Default to v6.0
   :type api_version: str
   :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
       https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
   :type fields: List[str]
   :param params: Parameters that determine the query for Facebook
       https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
   :type params: Dict[str, Any]
   :param sleep_time: Time to sleep when async call is happening
   :type sleep_time: int
   :param gzip: Option to compress local file or file data for upload
   :type gzip: bool
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
      :annotation: = ['facebook_conn_id', 'bucket_name', 'object_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




