:mod:`airflow.providers.amazon.aws.operators.s3_copy_object`
============================================================

.. py:module:: airflow.providers.amazon.aws.operators.s3_copy_object


Module Contents
---------------

.. py:class:: S3CopyObjectOperator(*, source_bucket_key: str, dest_bucket_key: str, source_bucket_name: Optional[str] = None, dest_bucket_name: Optional[str] = None, source_version_id: Optional[str] = None, aws_conn_id: str = 'aws_default', verify: Optional[Union[str, bool]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a copy of an object that is already stored in S3.

   Note: the S3 connection used here needs to have access to both
   source and destination bucket/key.

   :param source_bucket_key: The key of the source object. (templated)

       It can be either full s3:// style url or relative path from root level.

       When it's specified as a full s3:// url, please omit source_bucket_name.
   :type source_bucket_key: str
   :param dest_bucket_key: The key of the object to copy to. (templated)

       The convention to specify `dest_bucket_key` is the same as `source_bucket_key`.
   :type dest_bucket_key: str
   :param source_bucket_name: Name of the S3 bucket where the source object is in. (templated)

       It should be omitted when `source_bucket_key` is provided as a full s3:// url.
   :type source_bucket_name: str
   :param dest_bucket_name: Name of the S3 bucket to where the object is copied. (templated)

       It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
   :type dest_bucket_name: str
   :param source_version_id: Version ID of the source object (OPTIONAL)
   :type source_version_id: str
   :param aws_conn_id: Connection id of the S3 connection to use
   :type aws_conn_id: str
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.

       You can provide the following values:

       - False: do not validate SSL certificates. SSL will still be used,
                but SSL certificates will not be
                verified.
       - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :type verify: bool or str

   .. attribute:: template_fields
      :annotation: = ['source_bucket_key', 'dest_bucket_key', 'source_bucket_name', 'dest_bucket_name']

      

   
   .. method:: execute(self, context)




