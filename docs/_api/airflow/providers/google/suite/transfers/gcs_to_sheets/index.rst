:mod:`airflow.providers.google.suite.transfers.gcs_to_sheets`
=============================================================

.. py:module:: airflow.providers.google.suite.transfers.gcs_to_sheets


Module Contents
---------------

.. py:class:: GCSToGoogleSheetsOperator(*, spreadsheet_id: str, bucket_name: str, object_name: str, spreadsheet_range: str = 'Sheet1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Uploads .csv file from Google Cloud Storage to provided Google Spreadsheet.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToGoogleSheets`

   :param spreadsheet_id: The Google Sheet ID to interact with.
   :type spreadsheet_id: str
   :param bucket_name: Name of GCS bucket.:
   :type bucket_name: str
   :param object_name: Path to the .csv file on the GCS bucket.
   :type object_name: str
   :param spreadsheet_range: The A1 notation of the values to retrieve.
   :type spreadsheet_range: str
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
      :annotation: = ['spreadsheet_id', 'bucket_name', 'object_name', 'spreadsheet_range', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Any)




