:mod:`airflow.providers.amazon.aws.transfers.mysql_to_s3`
=========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.mysql_to_s3


Module Contents
---------------

.. py:class:: MySQLToS3Operator(*, query: str, s3_bucket: str, s3_key: str, mysql_conn_id: str = 'mysql_default', aws_conn_id: str = 'aws_default', verify: Optional[Union[bool, str]] = None, pd_csv_kwargs: Optional[dict] = None, index: bool = False, header: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Saves data from an specific MySQL query into a file in S3.

   :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
       ending with .sql extension. (templated)
   :type query: str
   :param s3_bucket: bucket where the data will be stored. (templated)
   :type s3_bucket: str
   :param s3_key: desired key for the file. It includes the name of the file. (templated)
   :type s3_key: str
   :param mysql_conn_id: reference to a specific mysql database
   :type mysql_conn_id: str
   :param aws_conn_id: reference to a specific S3 connection
   :type aws_conn_id: str
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
               (unless use_ssl is False), but SSL certificates will not be verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
               You can specify this argument if you want to use a different
               CA cert bundle than the one used by botocore.
   :type verify: bool or str
   :param pd_csv_kwargs: arguments to include in pd.to_csv (header, index, columns...)
   :type pd_csv_kwargs: dict
   :param index: whether to have the index or not in the dataframe
   :type index: str
   :param header: whether to include header or not into the S3 file
   :type header: bool

   .. attribute:: template_fields
      :annotation: = ['s3_bucket', 's3_key', 'query']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: _fix_int_dtypes(self, df: pd.DataFrame)

      Mutate DataFrame to set dtypes for int columns containing NaN values.



   
   .. method:: execute(self, context)




