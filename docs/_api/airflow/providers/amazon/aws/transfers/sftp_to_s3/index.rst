:mod:`airflow.providers.amazon.aws.transfers.sftp_to_s3`
========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.sftp_to_s3


Module Contents
---------------

.. py:class:: SFTPToS3Operator(*, s3_bucket: str, s3_key: str, sftp_path: str, sftp_conn_id: str = 'ssh_default', s3_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator enables the transferring of files from a SFTP server to
   Amazon S3.

   :param sftp_conn_id: The sftp connection id. The name or identifier for
       establishing a connection to the SFTP server.
   :type sftp_conn_id: str
   :param sftp_path: The sftp remote path. This is the specified file path
       for downloading the file from the SFTP server.
   :type sftp_path: str
   :param s3_conn_id: The s3 connection id. The name or identifier for
       establishing a connection to S3
   :type s3_conn_id: str
   :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
       the file is uploaded.
   :type s3_bucket: str
   :param s3_key: The targeted s3 key. This is the specified path for
       uploading the file to S3.
   :type s3_key: str

   .. attribute:: template_fields
      :annotation: = ['s3_key', 'sftp_path']

      

   
   .. staticmethod:: get_s3_key(s3_key: str)

      This parses the correct format for S3 keys regardless of how the S3 url is passed.



   
   .. method:: execute(self, context)




