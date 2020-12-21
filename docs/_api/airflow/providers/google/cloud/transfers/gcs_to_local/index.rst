:mod:`airflow.providers.google.cloud.transfers.gcs_to_local`
============================================================

.. py:module:: airflow.providers.google.cloud.transfers.gcs_to_local


Module Contents
---------------

.. py:class:: GCSToLocalFilesystemOperator(*, bucket: str, object_name: Optional[str] = None, filename: Optional[str] = None, store_to_xcom_key: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Downloads a file from Google Cloud Storage.

   If a filename is supplied, it writes the file to the specified location, alternatively one can
   set the ``store_to_xcom_key`` parameter to True push the file content into xcom. When the file size
   exceeds the maximum size for xcom it is recommended to write to a file.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToLocalFilesystemOperator`

   :param bucket: The Google Cloud Storage bucket where the object is.
       Must not contain 'gs://' prefix. (templated)
   :type bucket: str
   :param object: The name of the object to download in the Google cloud
       storage bucket. (templated)
   :type object: str
   :param filename: The file path, including filename,  on the local file system (where the
       operator is being executed) that the file should be downloaded to. (templated)
       If no filename passed, the downloaded data will not be stored on the local file
       system.
   :type filename: str
   :param store_to_xcom_key: If this param is set, the operator will push
       the contents of the downloaded file to XCom with the key set in this
       parameter. If not set, the downloaded data will not be pushed to XCom. (templated)
   :type store_to_xcom_key: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
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
      :annotation: = ['bucket', 'object', 'filename', 'store_to_xcom_key', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)




