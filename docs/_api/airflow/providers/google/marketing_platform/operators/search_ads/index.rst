:mod:`airflow.providers.google.marketing_platform.operators.search_ads`
=======================================================================

.. py:module:: airflow.providers.google.marketing_platform.operators.search_ads

.. autoapi-nested-parse::

   This module contains Google Search Ads operators.



Module Contents
---------------

.. py:class:: GoogleSearchAdsInsertReportOperator(*, report: Dict[str, Any], api_version: str = 'v2', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Inserts a report request into the reporting system.

   .. seealso:
       For API documentation check:
       https://developers.google.com/search-ads/v2/reference/reports/request

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleSearchAdsInsertReportOperator`

   :param report: Report to be generated
   :type report: Dict[str, Any]
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['report', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   
   .. method:: prepare_template(self)



   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleSearchAdsDownloadReportOperator(*, report_id: str, bucket_name: str, report_name: Optional[str] = None, gzip: bool = True, chunk_size: int = 10 * 1024 * 1024, api_version: str = 'v2', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Downloads a report to GCS bucket.

   .. seealso:
       For API documentation check:
       https://developers.google.com/search-ads/v2/reference/reports/getFile

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleSearchAdsGetfileReportOperator`

   :param report_id: ID of the report.
   :type report_id: str
   :param bucket_name: The bucket to upload to.
   :type bucket_name: str
   :param report_name: The report name to set when uploading the local file. If not provided then
       report_id is used.
   :type report_name: str
   :param gzip: Option to compress local file or file data for upload
   :type gzip: bool
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['report_name', 'report_id', 'bucket_name', 'impersonation_chain']

      

   
   .. method:: _resolve_file_name(self, name: str)



   
   .. staticmethod:: _set_bucket_name(name: str)



   
   .. staticmethod:: _handle_report_fragment(fragment: bytes)



   
   .. method:: execute(self, context: dict)




