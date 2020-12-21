:mod:`airflow.providers.amazon.aws.transfers.redshift_to_s3`
============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.redshift_to_s3

.. autoapi-nested-parse::

   Transfers data from AWS Redshift into a S3 Bucket.



Module Contents
---------------

.. py:class:: RedshiftToS3Operator(*, schema: str, table: str, s3_bucket: str, s3_key: str, redshift_conn_id: str = 'redshift_default', aws_conn_id: str = 'aws_default', verify: Optional[Union[bool, str]] = None, unload_options: Optional[List] = None, autocommit: bool = False, include_header: bool = False, table_as_file_name: bool = True, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes an UNLOAD command to s3 as a CSV with headers

   :param schema: reference to a specific schema in redshift database
   :type schema: str
   :param table: reference to a specific table in redshift database
   :type table: str
   :param s3_bucket: reference to a specific S3 bucket
   :type s3_bucket: str
   :param s3_key: reference to a specific S3 key. If ``table_as_file_name`` is set
       to False, this param must include the desired file name
   :type s3_key: str
   :param redshift_conn_id: reference to a specific redshift database
   :type redshift_conn_id: str
   :param aws_conn_id: reference to a specific S3 connection
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
   :param unload_options: reference to a list of UNLOAD options
   :type unload_options: list
   :param autocommit: If set to True it will automatically commit the UNLOAD statement.
       Otherwise it will be committed right before the redshift connection gets closed.
   :type autocommit: bool
   :param include_header: If set to True the s3 file contains the header columns.
   :type include_header: bool
   :param table_as_file_name: If set to True, the s3 file will be named as the table
   :type table_as_file_name: bool

   .. attribute:: template_fields
      :annotation: = ['s3_bucket', 's3_key', 'schema', 'table', 'unload_options']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: execute(self, context)




