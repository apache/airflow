:mod:`airflow.providers.google.cloud.transfers.local_to_gcs`
============================================================

.. py:module:: airflow.providers.google.cloud.transfers.local_to_gcs

.. autoapi-nested-parse::

   This module contains operator for uploading local file(s) to GCS.



Module Contents
---------------

.. py:class:: LocalFilesystemToGCSOperator(*, src, dst, bucket, gcp_conn_id='google_cloud_default', google_cloud_storage_conn_id=None, mime_type='application/octet-stream', delegate_to=None, gzip=False, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Uploads a file or list of files to Google Cloud Storage.
   Optionally can compress the file for upload.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:LocalFilesystemToGCSOperator`

   :param src: Path to the local file, or list of local files. Path can be either absolute
       (e.g. /path/to/file.ext) or relative (e.g. ../../foo/*/*.csv). (templated)
   :type src: str or list
   :param dst: Destination path within the specified bucket on GCS (e.g. /path/to/file.ext).
       If multiple files are being uploaded, specify object prefix with trailing backslash
       (e.g. /path/to/directory/) (templated)
   :type dst: str
   :param bucket: The bucket to upload to. (templated)
   :type bucket: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param mime_type: The mime-type string
   :type mime_type: str
   :param delegate_to: The account to impersonate, if any
   :type delegate_to: str
   :param gzip: Allows for file to be compressed and uploaded as gzip
   :type gzip: bool
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
      :annotation: = ['src', 'dst', 'bucket', 'impersonation_chain']

      

   
   .. method:: execute(self, context)

      Uploads a file or list of files to Google Cloud Storage




