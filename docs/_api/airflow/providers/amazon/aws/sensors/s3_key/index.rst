:mod:`airflow.providers.amazon.aws.sensors.s3_key`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.s3_key


Module Contents
---------------

.. py:class:: S3KeySensor(*, bucket_key: str, bucket_name: Optional[str] = None, wildcard_match: bool = False, aws_conn_id: str = 'aws_default', verify: Optional[Union[str, bool]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
   S3 being a key/value it does not support folders. The path is just a key
   a resource.

   :param bucket_key: The key being waited on. Supports full s3:// style url
       or relative path from root level. When it's specified as a full s3://
       url, please leave bucket_name as `None`.
   :type bucket_key: str
   :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
       is not provided as a full s3:// url.
   :type bucket_name: str
   :param wildcard_match: whether the bucket_key should be interpreted as a
       Unix wildcard pattern
   :type wildcard_match: bool
   :param aws_conn_id: a reference to the s3 connection
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

   .. attribute:: template_fields
      :annotation: = ['bucket_key', 'bucket_name']

      

   
   .. method:: poke(self, context)



   
   .. method:: get_hook(self)

      Create and return an S3Hook




