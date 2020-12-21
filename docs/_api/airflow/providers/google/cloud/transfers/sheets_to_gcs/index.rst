:mod:`airflow.providers.google.cloud.transfers.sheets_to_gcs`
=============================================================

.. py:module:: airflow.providers.google.cloud.transfers.sheets_to_gcs


Module Contents
---------------

.. py:class:: GoogleSheetsToGCSOperator(*, spreadsheet_id: str, destination_bucket: str, sheet_filter: Optional[List[str]] = None, destination_path: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Writes Google Sheet data into Google Cloud Storage.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleSheetsToGCSOperator`

   :param spreadsheet_id: The Google Sheet ID to interact with.
   :type spreadsheet_id: str
   :param sheet_filter: Default to None, if provided, Should be an array of the sheet
       titles to pull from.
   :type sheet_filter: List[str]
   :param destination_bucket: The destination Google cloud storage bucket where the
       report should be written to. (templated)
   :param destination_bucket: str
   :param destination_path: The Google cloud storage URI array for the object created by the operator.
       For example: ``path/to/my/files``.
   :type destination_path: str
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
      :annotation: = ['spreadsheet_id', 'destination_bucket', 'destination_path', 'sheet_filter', 'impersonation_chain']

      

   
   .. method:: _upload_data(self, gcs_hook: GCSHook, hook: GSheetsHook, sheet_range: str, sheet_values: List[Any])



   
   .. method:: execute(self, context)




