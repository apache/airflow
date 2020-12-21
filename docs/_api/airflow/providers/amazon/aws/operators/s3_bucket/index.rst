:mod:`airflow.providers.amazon.aws.operators.s3_bucket`
=======================================================

.. py:module:: airflow.providers.amazon.aws.operators.s3_bucket

.. autoapi-nested-parse::

   This module contains AWS S3 operators.



Module Contents
---------------

.. py:class:: S3CreateBucketOperator(*, bucket_name: str, aws_conn_id: Optional[str] = 'aws_default', region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator creates an S3 bucket

   :param bucket_name: This is bucket name you want to create
   :type bucket_name: str
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).
   :type aws_conn_id: Optional[str]
   :param region_name: AWS region_name. If not specified fetched from connection.
   :type region_name: Optional[str]

   
   .. method:: execute(self, context)




.. py:class:: S3DeleteBucketOperator(bucket_name: str, force_delete: bool = False, aws_conn_id: Optional[str] = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator deletes an S3 bucket

   :param bucket_name: This is bucket name you want to delete
   :type bucket_name: str
   :param force_delete: Forcibly delete all objects in the bucket before deleting the bucket
   :type force_delete: bool
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).
   :type aws_conn_id: Optional[str]

   
   .. method:: execute(self, context)




