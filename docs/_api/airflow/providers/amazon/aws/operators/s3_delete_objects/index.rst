:mod:`airflow.providers.amazon.aws.operators.s3_delete_objects`
===============================================================

.. py:module:: airflow.providers.amazon.aws.operators.s3_delete_objects


Module Contents
---------------

.. py:class:: S3DeleteObjectsOperator(*, bucket: str, keys: Optional[Union[str, list]] = None, prefix: Optional[str] = None, aws_conn_id: str = 'aws_default', verify: Optional[Union[str, bool]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   To enable users to delete single object or multiple objects from
   a bucket using a single HTTP request.

   Users may specify up to 1000 keys to delete.

   :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
   :type bucket: str
   :param keys: The key(s) to delete from S3 bucket. (templated)

       When ``keys`` is a string, it's supposed to be the key name of
       the single object to delete.

       When ``keys`` is a list, it's supposed to be the list of the
       keys to delete.

       You may specify up to 1000 keys.
   :type keys: str or list
   :param prefix: Prefix of objects to delete. (templated)
       All objects matching this prefix in the bucket will be deleted.
   :type prefix: str
   :param aws_conn_id: Connection id of the S3 connection to use
   :type aws_conn_id: str
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.

       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used,
                but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :type verify: bool or str

   .. attribute:: template_fields
      :annotation: = ['keys', 'bucket', 'prefix']

      

   
   .. method:: execute(self, context)




