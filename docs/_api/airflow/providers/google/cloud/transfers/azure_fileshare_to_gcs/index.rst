:mod:`airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs`
======================================================================

.. py:module:: airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs


Module Contents
---------------

.. py:class:: AzureFileShareToGCSOperator(*, share_name: str, dest_gcs: str, directory_name: Optional[str] = None, prefix: str = '', wasb_conn_id: str = 'wasb_default', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, replace: bool = False, gzip: bool = False, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Synchronizes a Azure FileShare directory content (excluding subdirectories),
   possibly filtered by a prefix, with a Google Cloud Storage destination path.

   :param share_name: The Azure FileShare share where to find the objects. (templated)
   :type share_name: str
   :param directory_name: (Optional) Path to Azure FileShare directory which content is to be transferred.
       Defaults to root directory (templated)
   :type directory_name: str
   :param prefix: Prefix string which filters objects whose name begin with
       such prefix. (templated)
   :type prefix: str
   :param wasb_conn_id: The source WASB connection
   :type wasb_conn_id: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param dest_gcs_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type dest_gcs_conn_id: str
   :param dest_gcs: The destination Google Cloud Storage bucket and prefix
       where you want to store the files. (templated)
   :type dest_gcs: str
   :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param replace: Whether you want to replace existing destination files
       or not.
   :type replace: bool
   :param gzip: Option to compress file for upload
   :type gzip: bool
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Optional[Union[str, Sequence[str]]]

   Note that ``share_name``, ``directory_name``, ``prefix``, ``delimiter`` and ``dest_gcs`` are
   templated, so you can use variables in them if you wish.

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['share_name', 'directory_name', 'prefix', 'dest_gcs']

      

   
   .. method:: execute(self, context)




