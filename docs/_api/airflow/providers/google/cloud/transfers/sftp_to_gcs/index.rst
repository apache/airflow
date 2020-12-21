:mod:`airflow.providers.google.cloud.transfers.sftp_to_gcs`
===========================================================

.. py:module:: airflow.providers.google.cloud.transfers.sftp_to_gcs

.. autoapi-nested-parse::

   This module contains SFTP to Google Cloud Storage operator.



Module Contents
---------------

.. data:: WILDCARD
   :annotation: = *

   

.. py:class:: SFTPToGCSOperator(*, source_path: str, destination_bucket: str, destination_path: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', sftp_conn_id: str = 'ssh_default', delegate_to: Optional[str] = None, mime_type: str = 'application/octet-stream', gzip: bool = False, move_object: bool = False, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Transfer files to Google Cloud Storage from SFTP server.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SFTPToGCSOperator`

   :param source_path: The sftp remote path. This is the specified file path
       for downloading the single file or multiple files from the SFTP server.
       You can use only one wildcard within your path. The wildcard can appear
       inside the path or at the end of the path.
   :type source_path: str
   :param destination_bucket: The bucket to upload to.
   :type destination_bucket: str
   :param destination_path: The destination name of the object in the
       destination Google Cloud Storage bucket.
       If destination_path is not provided file/files will be placed in the
       main bucket path.
       If a wildcard is supplied in the destination_path argument, this is the
       prefix that will be prepended to the final destination objects' paths.
   :type destination_path: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param sftp_conn_id: The sftp connection id. The name or identifier for
       establishing a connection to the SFTP server.
   :type sftp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param mime_type: The mime-type string
   :type mime_type: str
   :param gzip: Allows for file to be compressed and uploaded as gzip
   :type gzip: bool
   :param move_object: When move object is True, the object is moved instead
       of copied to the new location. This is the equivalent of a mv command
       as opposed to a cp command.
   :type move_object: bool
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
      :annotation: = ['source_path', 'destination_path', 'destination_bucket', 'impersonation_chain']

      

   
   .. method:: execute(self, context)



   
   .. method:: _copy_single_object(self, gcs_hook: GCSHook, sftp_hook: SFTPHook, source_path: str, destination_object: str)

      Helper function to copy single object.



   
   .. staticmethod:: _set_destination_path(path: Union[str, None])



   
   .. staticmethod:: _set_bucket_name(name: str)




