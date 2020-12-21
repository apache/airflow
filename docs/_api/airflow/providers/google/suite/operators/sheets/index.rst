:mod:`airflow.providers.google.suite.operators.sheets`
======================================================

.. py:module:: airflow.providers.google.suite.operators.sheets


Module Contents
---------------

.. py:class:: GoogleSheetsCreateSpreadsheetOperator(*, spreadsheet: Dict[str, Any], gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new spreadsheet.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleSheetsCreateSpreadsheetOperator`

   :param spreadsheet: an instance of Spreadsheet
       https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet
   :type spreadsheet: Dict[str, Any]
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
      :annotation: = ['spreadsheet', 'impersonation_chain']

      

   
   .. method:: execute(self, context: Any)




