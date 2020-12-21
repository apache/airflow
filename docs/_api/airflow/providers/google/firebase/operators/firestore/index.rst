:mod:`airflow.providers.google.firebase.operators.firestore`
============================================================

.. py:module:: airflow.providers.google.firebase.operators.firestore


Module Contents
---------------

.. py:class:: CloudFirestoreExportDatabaseOperator(*, body: Dict, database_id: str = '(default)', project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Exports a copy of all or a subset of documents from Google Cloud Firestore to another storage system,
   such as Google Cloud Storage.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudFirestoreExportDatabaseOperator`

   :param database_id: The Database ID.
   :type database_id: str
   :param body: The request body.
       See:
       https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases/exportDocuments
   :type body: dict
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

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




