:mod:`airflow.providers.google.cloud.transfers.s3_to_gcs`
=========================================================

.. py:module:: airflow.providers.google.cloud.transfers.s3_to_gcs


Module Contents
---------------

.. py:class:: S3ToGCSOperator(*, bucket, prefix='', delimiter='', aws_conn_id='aws_default', verify=None, gcp_conn_id='google_cloud_default', dest_gcs_conn_id=None, dest_gcs=None, delegate_to=None, replace=False, gzip=False, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.operators.s3_list.S3ListOperator`

   Synchronizes an S3 key, possibly a prefix, with a Google Cloud Storage
   destination path.

   :param bucket: The S3 bucket where to find the objects. (templated)
   :type bucket: str
   :param prefix: Prefix string which filters objects whose name begin with
       such prefix. (templated)
   :type prefix: str
   :param delimiter: the delimiter marks key hierarchy. (templated)
   :type delimiter: str
   :param aws_conn_id: The source S3 connection
   :type aws_conn_id: str
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :type verify: bool or str
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
   :type google_impersonation_chain: Union[str, Sequence[str]]


   **Example**:

   .. code-block:: python

      s3_to_gcs_op = S3ToGCSOperator(
           task_id='s3_to_gcs_example',
           bucket='my-s3-bucket',
           prefix='data/customers-201804',
           dest_gcs_conn_id='google_cloud_default',
           dest_gcs='gs://my.gcs.bucket/some/customers/',
           replace=False,
           gzip=True,
           dag=my-dag)

   Note that ``bucket``, ``prefix``, ``delimiter`` and ``dest_gcs`` are
   templated, so you can use variables in them if you wish.

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['bucket', 'prefix', 'delimiter', 'dest_gcs', 'google_impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e09411

      

   
   .. method:: execute(self, context)




