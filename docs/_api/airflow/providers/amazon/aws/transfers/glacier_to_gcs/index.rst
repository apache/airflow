:mod:`airflow.providers.amazon.aws.transfers.glacier_to_gcs`
============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.glacier_to_gcs


Module Contents
---------------

.. py:class:: GlacierToGCSOperator(*, aws_conn_id: str = 'aws_default', gcp_conn_id: str = 'google_cloud_default', vault_name: str, bucket_name: str, object_name: str, gzip: bool, chunk_size: int = 1024, delegate_to: Optional[str] = None, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Transfers data from Amazon Glacier to Google Cloud Storage

   .. note::
       Please be warn that GlacierToGCSOperator may depends on memory usage.
       Transferring big files may not working well.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GlacierToGCSOperator`

   :param aws_conn_id: The reference to the AWS connection details
   :type aws_conn_id: str
   :param gcp_conn_id: The reference to the GCP connection details
   :type gcp_conn_id: str
   :param vault_name: the Glacier vault on which job is executed
   :type vault_name: string
   :param bucket_name: the Google Cloud Storage bucket where the data will be transferred
   :type bucket_name: str
   :param object_name: the name of the object to check in the Google cloud
       storage bucket.
   :type object_name: str
   :param gzip: option to compress local file or file data for upload
   :type gzip: bool
   :param chunk_size: size of chunk in bytes the that will downloaded from Glacier vault
   :type chunk_size: int
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
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
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['vault_name', 'bucket_name', 'object_name']

      

   
   .. method:: execute(self, context)




