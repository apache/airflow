:mod:`airflow.providers.amazon.aws.transfers.s3_to_redshift`
============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.s3_to_redshift


Module Contents
---------------

.. py:class:: S3ToRedshiftOperator(*, schema: str, table: str, s3_bucket: str, s3_key: str, redshift_conn_id: str = 'redshift_default', aws_conn_id: str = 'aws_default', verify: Optional[Union[bool, str]] = None, copy_options: Optional[List] = None, autocommit: bool = False, truncate_table: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes an COPY command to load files from s3 to Redshift

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3ToRedshiftOperator`

   :param schema: reference to a specific schema in redshift database
   :type schema: str
   :param table: reference to a specific table in redshift database
   :type table: str
   :param s3_bucket: reference to a specific S3 bucket
   :type s3_bucket: str
   :param s3_key: reference to a specific S3 key
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
   :param copy_options: reference to a list of COPY options
   :type copy_options: list
   :param truncate_table: whether or not to truncate the destination table before the copy
   :type truncate_table: bool

   .. attribute:: template_fields
      :annotation: = ['s3_bucket', 's3_key', 'schema', 'table', 'copy_options']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #99e699

      

   
   .. method:: execute(self, context)




