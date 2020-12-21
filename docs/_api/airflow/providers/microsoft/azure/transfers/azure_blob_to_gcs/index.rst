:mod:`airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs`
====================================================================

.. py:module:: airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs


Module Contents
---------------

.. py:class:: AzureBlobStorageToGCSOperator(*, wasb_conn_id='wasb_default', gcp_conn_id: str = 'google_cloud_default', blob_name: str, file_path: str, container_name: str, bucket_name: str, object_name: str, filename: str, gzip: bool, delegate_to: Optional[str], impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator transfers data from Azure Blob Storage to specified bucket in Google Cloud Storage

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AzureBlobStorageToGCSOperator`

   :param wasb_conn_id: Reference to the wasb connection.
   :type wasb_conn_id: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param blob_name: Name of the blob
   :type blob_name: str
   :param file_path: Path to the file to download
   :type file_path: str
   :param container_name: Name of the container
   :type container_name: str
   :param bucket_name: The bucket to upload to
   :type bucket_name: str
   :param object_name: The object name to set when uploading the file
   :type object_name: str
   :param filename: The local file path to the file to be uploaded
   :type filename: str
   :param gzip: Option to compress local file or file data for upload
   :type gzip: bool
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
       account from the list granting this role to the originating account.
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['blob_name', 'file_path', 'container_name', 'bucket_name', 'object_name', 'filename']

      

   
   .. method:: execute(self, context: dict)




