:mod:`airflow.providers.amazon.aws.sensors.s3_prefix`
=====================================================

.. py:module:: airflow.providers.amazon.aws.sensors.s3_prefix


Module Contents
---------------

.. py:class:: S3PrefixSensor(*, bucket_name: str, prefix: str, delimiter: str = '/', aws_conn_id: str = 'aws_default', verify: Optional[Union[str, bool]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a prefix to exist. A prefix is the first part of a key,
   thus enabling checking of constructs similar to glob airfl* or
   SQL LIKE 'airfl%'. There is the possibility to precise a delimiter to
   indicate the hierarchy or keys, meaning that the match will stop at that
   delimiter. Current code accepts sane delimiters, i.e. characters that
   are NOT special characters in the Python regex engine.

   :param bucket_name: Name of the S3 bucket
   :type bucket_name: str
   :param prefix: The prefix being waited on. Relative path from bucket root level.
   :type prefix: str
   :param delimiter: The delimiter intended to show hierarchy.
       Defaults to '/'.
   :type delimiter: str
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
      :annotation: = ['prefix', 'bucket_name']

      

   
   .. method:: poke(self, context)



   
   .. method:: get_hook(self)

      Create and return an S3Hook




