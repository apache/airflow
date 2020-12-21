:mod:`airflow.providers.amazon.aws.operators.s3_list`
=====================================================

.. py:module:: airflow.providers.amazon.aws.operators.s3_list


Module Contents
---------------

.. py:class:: S3ListOperator(*, bucket: str, prefix: str = '', delimiter: str = '', aws_conn_id: str = 'aws_default', verify: Optional[Union[str, bool]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   List all objects from the bucket with the given string prefix in name.

   This operator returns a python list with the name of objects which can be
   used by `xcom` in the downstream task.

   :param bucket: The S3 bucket where to find the objects. (templated)
   :type bucket: str
   :param prefix: Prefix string to filters the objects whose name begin with
       such prefix. (templated)
   :type prefix: str
   :param delimiter: the delimiter marks key hierarchy. (templated)
   :type delimiter: str
   :param aws_conn_id: The connection ID to use when connecting to S3 storage.
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


   **Example**:
       The following operator would list all the files
       (excluding subfolders) from the S3
       ``customers/2018/04/`` key in the ``data`` bucket. ::

           s3_file = S3ListOperator(
               task_id='list_3s_files',
               bucket='data',
               prefix='customers/2018/04/',
               delimiter='/',
               aws_conn_id='aws_customers_conn'
           )

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['bucket', 'prefix', 'delimiter']

      

   .. attribute:: ui_color
      :annotation: = #ffd700

      

   
   .. method:: execute(self, context)




