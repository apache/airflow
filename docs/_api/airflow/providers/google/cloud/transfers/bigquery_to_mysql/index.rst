:mod:`airflow.providers.google.cloud.transfers.bigquery_to_mysql`
=================================================================

.. py:module:: airflow.providers.google.cloud.transfers.bigquery_to_mysql

.. autoapi-nested-parse::

   This module contains Google BigQuery to MySQL operator.



Module Contents
---------------

.. py:class:: BigQueryToMySqlOperator(*, dataset_table: str, mysql_table: str, selected_fields: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', mysql_conn_id: str = 'mysql_default', database: Optional[str] = None, delegate_to: Optional[str] = None, replace: bool = False, batch_size: int = 1000, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
   and insert that data into a MySQL table.


   .. note::
       If you pass fields to ``selected_fields`` which are in different order than the
       order of columns already in
       BQ table, the data will still be in the order of BQ table.
       For example if the BQ table has 3 columns as
       ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
       the data would still be of the form ``'A,B'`` and passed through this form
       to MySQL

   **Example**: ::

      transfer_data = BigQueryToMySqlOperator(
           task_id='task_id',
           dataset_table='origin_bq_table',
           mysql_table='dest_table_name',
           replace=True,
       )

   :param dataset_table: A dotted ``<dataset>.<table>``: the big query table of origin
   :type dataset_table: str
   :param max_results: The maximum number of records (rows) to be fetched
       from the table. (templated)
   :type max_results: str
   :param selected_fields: List of fields to return (comma-separated). If
       unspecified, all fields are returned.
   :type selected_fields: str
   :param gcp_conn_id: reference to a specific Google Cloud hook.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :type delegate_to: str
   :param mysql_conn_id: reference to a specific mysql hook
   :type mysql_conn_id: str
   :param database: name of database which overwrite defined one in connection
   :type database: str
   :param replace: Whether to replace instead of insert
   :type replace: bool
   :param batch_size: The number of rows to take in each batch
   :type batch_size: int
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
      :annotation: = ['dataset_id', 'table_id', 'mysql_table', 'impersonation_chain']

      

   
   .. method:: _bq_get_data(self)



   
   .. method:: execute(self, context)




