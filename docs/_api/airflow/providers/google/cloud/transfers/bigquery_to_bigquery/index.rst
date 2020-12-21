:mod:`airflow.providers.google.cloud.transfers.bigquery_to_bigquery`
====================================================================

.. py:module:: airflow.providers.google.cloud.transfers.bigquery_to_bigquery

.. autoapi-nested-parse::

   This module contains Google BigQuery to BigQuery operator.



Module Contents
---------------

.. py:class:: BigQueryToBigQueryOperator(*, source_project_dataset_tables: Union[List[str], str], destination_project_dataset_table: str, write_disposition: str = 'WRITE_EMPTY', create_disposition: str = 'CREATE_IF_NEEDED', gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, labels: Optional[Dict] = None, encryption_configuration: Optional[Dict] = None, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Copies data from one BigQuery table to another.

   .. seealso::
       For more details about these parameters:
       https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.copy

   :param source_project_dataset_tables: One or more
       dotted ``(project:|project.)<dataset>.<table>`` BigQuery tables to use as the
       source data. If ``<project>`` is not included, project will be the
       project defined in the connection json. Use a list if there are multiple
       source tables. (templated)
   :type source_project_dataset_tables: list|string
   :param destination_project_dataset_table: The destination BigQuery
       table. Format is: ``(project:|project.)<dataset>.<table>`` (templated)
   :type destination_project_dataset_table: str
   :param write_disposition: The write disposition if the table already exists.
   :type write_disposition: str
   :param create_disposition: The create disposition if the table doesn't exist.
   :type create_disposition: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param labels: a dictionary containing labels for the job/query,
       passed to BigQuery
   :type labels: dict
   :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
       **Example**: ::

           encryption_configuration = {
               "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
           }
   :type encryption_configuration: dict
   :param location: The location used for the operation.
   :type location: str
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
      :annotation: = ['source_project_dataset_tables', 'destination_project_dataset_table', 'labels', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #e6f0e4

      

   
   .. method:: execute(self, context)




