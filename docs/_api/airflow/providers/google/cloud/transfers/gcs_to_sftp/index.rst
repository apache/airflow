:mod:`airflow.providers.google.cloud.transfers.gcs_to_sftp`
===========================================================

.. py:module:: airflow.providers.google.cloud.transfers.gcs_to_sftp

.. autoapi-nested-parse::

   This module contains Google Cloud Storage to SFTP operator.



Module Contents
---------------

.. data:: WILDCARD
   :annotation: = *

   

.. py:class:: GCSToSFTPOperator(*, source_bucket: str, source_object: str, destination_path: str, move_object: bool = False, gcp_conn_id: str = 'google_cloud_default', sftp_conn_id: str = 'ssh_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Transfer files from a Google Cloud Storage bucket to SFTP server.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToSFTPOperator`

   :param source_bucket: The source Google Cloud Storage bucket where the
        object is. (templated)
   :type source_bucket: str
   :param source_object: The source name of the object to copy in the Google cloud
       storage bucket. (templated)
       You can use only one wildcard for objects (filenames) within your
       bucket. The wildcard can appear inside the object name or at the
       end of the object name. Appending a wildcard to the bucket name is
       unsupported.
   :type source_object: str
   :param destination_path: The sftp remote path. This is the specified directory path for
       uploading to the SFTP server.
   :type destination_path: str
   :param move_object: When move object is True, the object is moved instead
       of copied to the new location. This is the equivalent of a mv command
       as opposed to a cp command.
   :type move_object: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param sftp_conn_id: The sftp connection id. The name or identifier for
       establishing a connection to the SFTP server.
   :type sftp_conn_id: str
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
      :annotation: = ['source_bucket', 'source_object', 'destination_path', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)



   
   .. method:: _copy_single_object(self, gcs_hook: GCSHook, sftp_hook: SFTPHook, source_object: str, destination_path: str)

      Helper function to copy single object.




