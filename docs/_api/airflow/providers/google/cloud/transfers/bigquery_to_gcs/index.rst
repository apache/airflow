:mod:`airflow.providers.google.cloud.transfers.bigquery_to_gcs`
===============================================================

.. py:module:: airflow.providers.google.cloud.transfers.bigquery_to_gcs

.. autoapi-nested-parse::

   This module contains Google BigQuery to Google Cloud Storage operator.



Module Contents
---------------

.. py:class:: BigQueryToGCSOperator(*, source_project_dataset_table: str, destination_cloud_storage_uris: List[str], compression: str = 'NONE', export_format: str = 'CSV', field_delimiter: str = ',', print_header: bool = True, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, labels: Optional[Dict] = None, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Transfers a BigQuery table to a Google Cloud Storage bucket.

   .. seealso::
       For more details about these parameters:
       https://cloud.google.com/bigquery/docs/reference/v2/jobs

   :param source_project_dataset_table: The dotted
       ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to use as the
       source data. If ``<project>`` is not included, project will be the project
       defined in the connection json. (templated)
   :type source_project_dataset_table: str
   :param destination_cloud_storage_uris: The destination Google Cloud
       Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
       convention defined here:
       https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
   :type destination_cloud_storage_uris: List[str]
   :param compression: Type of compression to use.
   :type compression: str
   :param export_format: File format to export.
   :type export_format: str
   :param field_delimiter: The delimiter to use when extracting to a CSV.
   :type field_delimiter: str
   :param print_header: Whether to print a header for a CSV file extract.
   :type print_header: bool
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
      :annotation: = ['source_project_dataset_table', 'destination_cloud_storage_uris', 'labels', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #e4e6f0

      

   
   .. method:: execute(self, context)




