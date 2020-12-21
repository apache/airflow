:mod:`airflow.providers.google.cloud.transfers.adls_to_gcs`
===========================================================

.. py:module:: airflow.providers.google.cloud.transfers.adls_to_gcs

.. autoapi-nested-parse::

   This module contains Azure Data Lake Storage to
   Google Cloud Storage operator.



Module Contents
---------------

.. py:class:: ADLSToGCSOperator(*, src_adls: str, dest_gcs: str, azure_data_lake_conn_id: str, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, replace: bool = False, gzip: bool = False, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.microsoft.azure.operators.adls_list.AzureDataLakeStorageListOperator`

   Synchronizes an Azure Data Lake Storage path with a GCS bucket

   :param src_adls: The Azure Data Lake path to find the objects (templated)
   :type src_adls: str
   :param dest_gcs: The Google Cloud Storage bucket and prefix to
       store the objects. (templated)
   :type dest_gcs: str
   :param replace: If true, replaces same-named files in GCS
   :type replace: bool
   :param gzip: Option to compress file for upload
   :type gzip: bool
   :param azure_data_lake_conn_id: The connection ID to use when
       connecting to Azure Data Lake Storage.
   :type azure_data_lake_conn_id: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]

   **Examples**:
       The following Operator would copy a single file named
       ``hello/world.avro`` from ADLS to the GCS bucket ``mybucket``. Its full
       resulting gcs path will be ``gs://mybucket/hello/world.avro`` ::

           copy_single_file = AdlsToGoogleCloudStorageOperator(
               task_id='copy_single_file',
               src_adls='hello/world.avro',
               dest_gcs='gs://mybucket',
               replace=False,
               azure_data_lake_conn_id='azure_data_lake_default',
               gcp_conn_id='google_cloud_default'
           )

       The following Operator would copy all parquet files from ADLS
       to the GCS bucket ``mybucket``. ::

           copy_all_files = AdlsToGoogleCloudStorageOperator(
               task_id='copy_all_files',
               src_adls='*.parquet',
               dest_gcs='gs://mybucket',
               replace=False,
               azure_data_lake_conn_id='azure_data_lake_default',
               gcp_conn_id='google_cloud_default'
           )

        The following Operator would copy all parquet files from ADLS
        path ``/hello/world``to the GCS bucket ``mybucket``. ::

           copy_world_files = AdlsToGoogleCloudStorageOperator(
               task_id='copy_world_files',
               src_adls='hello/world/*.parquet',
               dest_gcs='gs://mybucket',
               replace=False,
               azure_data_lake_conn_id='azure_data_lake_default',
               gcp_conn_id='google_cloud_default'
           )

   .. attribute:: template_fields
      :annotation: :Sequence[str] = ['src_adls', 'dest_gcs', 'google_impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)




