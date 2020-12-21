:mod:`airflow.providers.google.ads.transfers.ads_to_gcs`
========================================================

.. py:module:: airflow.providers.google.ads.transfers.ads_to_gcs


Module Contents
---------------

.. py:class:: GoogleAdsToGcsOperator(*, client_ids: List[str], query: str, attributes: List[str], bucket: str, obj: str, gcp_conn_id: str = 'google_cloud_default', google_ads_conn_id: str = 'google_ads_default', page_size: int = 10000, gzip: bool = False, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Fetches the daily results from the Google Ads API for 1-n clients
   Converts and saves the data as a temporary CSV file
   Uploads the CSV to Google Cloud Storage

   .. seealso::
       For more information on the Google Ads API, take a look at the API docs:
       https://developers.google.com/google-ads/api/docs/start

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleAdsToGcsOperator`

   :param client_ids: Google Ads client IDs to query
   :type client_ids: List[str]
   :param query: Google Ads Query Language API query
   :type query: str
   :param attributes: List of Google Ads Row attributes to extract
   :type attributes: List[str]
   :param bucket: The GCS bucket to upload to
   :type bucket: str
   :param obj: GCS path to save the object. Must be the full file path (ex. `path/to/file.txt`)
   :type obj: str
   :param gcp_conn_id: Airflow Google Cloud connection ID
   :type gcp_conn_id: str
   :param google_ads_conn_id: Airflow Google Ads connection ID
   :type google_ads_conn_id: str
   :param page_size: The number of results per API page request. Max 10,000
   :type page_size: int
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
      :annotation: = ['client_ids', 'query', 'attributes', 'bucket', 'obj', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




