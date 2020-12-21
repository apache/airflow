:mod:`airflow.providers.amazon.aws.transfers.gcs_to_s3`
=======================================================

.. py:module:: airflow.providers.amazon.aws.transfers.gcs_to_s3

.. autoapi-nested-parse::

   This module contains Google Cloud Storage to S3 operator.



Module Contents
---------------

.. py:class:: GCSToS3Operator(*, bucket: str, prefix: Optional[str] = None, delimiter: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, dest_aws_conn_id: str = 'aws_default', dest_s3_key: str, dest_verify: Optional[Union[str, bool]] = None, replace: bool = False, google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None, dest_s3_extra_args: Optional[Dict] = None, s3_acl_policy: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Synchronizes a Google Cloud Storage bucket with an S3 bucket.

   :param bucket: The Google Cloud Storage bucket to find the objects. (templated)
   :type bucket: str
   :param prefix: Prefix string which filters objects whose name begin with
       this prefix. (templated)
   :type prefix: str
   :param delimiter: The delimiter by which you want to filter the objects. (templated)
       For e.g to lists the CSV files from in a directory in GCS you would use
       delimiter='.csv'.
   :type delimiter: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param dest_aws_conn_id: The destination S3 connection
   :type dest_aws_conn_id: str
   :param dest_s3_key: The base S3 key to be used to store the files. (templated)
   :type dest_s3_key: str
   :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.

   :type dest_verify: bool or str
   :param replace: Whether or not to verify the existence of the files in the
       destination bucket.
       By default is set to False
       If set to True, will upload all the files replacing the existing ones in
       the destination bucket.
       If set to False, will upload only the files that are in the origin but not
       in the destination bucket.
   :type replace: bool
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type google_impersonation_chain: Union[str, Sequence[str]]
   :param s3_acl_policy: Optional The string to specify the canned ACL policy for the
       object to be uploaded in S3
   :type s3_acl_policy: str

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['bucket', 'prefix', 'delimiter', 'dest_s3_key', 'google_impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)




