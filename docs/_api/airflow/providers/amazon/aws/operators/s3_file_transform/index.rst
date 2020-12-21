:mod:`airflow.providers.amazon.aws.operators.s3_file_transform`
===============================================================

.. py:module:: airflow.providers.amazon.aws.operators.s3_file_transform


Module Contents
---------------

.. py:class:: S3FileTransformOperator(*, source_s3_key: str, dest_s3_key: str, transform_script: Optional[str] = None, select_expression=None, script_args: Optional[Sequence[str]] = None, source_aws_conn_id: str = 'aws_default', source_verify: Optional[Union[bool, str]] = None, dest_aws_conn_id: str = 'aws_default', dest_verify: Optional[Union[bool, str]] = None, replace: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Copies data from a source S3 location to a temporary location on the
   local filesystem. Runs a transformation on this file as specified by
   the transformation script and uploads the output to a destination S3
   location.

   The locations of the source and the destination files in the local
   filesystem is provided as an first and second arguments to the
   transformation script. The transformation script is expected to read the
   data from source, transform it and write the output to the local
   destination file. The operator then takes over control and uploads the
   local destination file to S3.

   S3 Select is also available to filter the source contents. Users can
   omit the transformation script if S3 Select expression is specified.

   :param source_s3_key: The key to be retrieved from S3. (templated)
   :type source_s3_key: str
   :param dest_s3_key: The key to be written from S3. (templated)
   :type dest_s3_key: str
   :param transform_script: location of the executable transformation script
   :type transform_script: str
   :param select_expression: S3 Select expression
   :type select_expression: str
   :param script_args: arguments for transformation script (templated)
   :type script_args: sequence of str
   :param source_aws_conn_id: source s3 connection
   :type source_aws_conn_id: str
   :param source_verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
            (unless use_ssl is False), but SSL certificates will not be
            verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
            You can specify this argument if you want to use a different
            CA cert bundle than the one used by botocore.

       This is also applicable to ``dest_verify``.
   :type source_verify: bool or str
   :param dest_aws_conn_id: destination s3 connection
   :type dest_aws_conn_id: str
   :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
       See: ``source_verify``
   :type dest_verify: bool or str
   :param replace: Replace dest S3 key if it already exists
   :type replace: bool

   .. attribute:: template_fields
      :annotation: = ['source_s3_key', 'dest_s3_key', 'script_args']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #f9c915

      

   
   .. method:: execute(self, context)




