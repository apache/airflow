:mod:`airflow.providers.google.suite.transfers.gcs_to_gdrive`
=============================================================

.. py:module:: airflow.providers.google.suite.transfers.gcs_to_gdrive

.. autoapi-nested-parse::

   This module contains a Google Cloud Storage to Google Drive transfer operator.



Module Contents
---------------

.. data:: WILDCARD
   :annotation: = *

   

.. py:class:: GCSToGoogleDriveOperator(*, source_bucket: str, source_object: str, destination_object: Optional[str] = None, move_object: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Copies objects from a Google Cloud Storage service service to Google Drive service, with renaming
   if requested.

   Using this operator requires the following OAuth 2.0 scope:

   .. code-block:: none

       https://www.googleapis.com/auth/drive

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToGoogleDriveOperator`

   :param source_bucket: The source Google Cloud Storage bucket where the object is. (templated)
   :type source_bucket: str
   :param source_object: The source name of the object to copy in the Google cloud
       storage bucket. (templated)
       You can use only one wildcard for objects (filenames) within your bucket. The wildcard can appear
       inside the object name or at the end of the object name. Appending a wildcard to the bucket name
       is unsupported.
   :type source_object: str
   :param destination_object: The destination name of the object in the destination Google Drive
       service. (templated)
       If a wildcard is supplied in the source_object argument, this is the prefix that will be prepended
       to the final destination objects' paths.
       Note that the source path's part before the wildcard will be removed;
       if it needs to be retained it should be appended to destination_object.
       For example, with prefix ``foo/*`` and destination_object ``blah/``, the file ``foo/baz`` will be
       copied to ``blah/baz``; to retain the prefix write the destination_object as e.g. ``blah/foo``, in
       which case the copied file will be named ``blah/foo/baz``.
   :type destination_object: str
   :param move_object: When move object is True, the object is moved instead of copied to the new location.
       This is the equivalent of a mv command as opposed to a cp command.
   :type move_object: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['source_bucket', 'source_object', 'destination_object', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)



   
   .. method:: _copy_single_object(self, source_object, destination_object)




