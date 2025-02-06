 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.amazon.aws.operators.s3`
===================================================

.. py:module:: airflow.providers.amazon.aws.operators.s3

.. autoapi-nested-parse::

   This module contains AWS S3 operators.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.s3.S3CreateBucketOperator
   airflow.providers.amazon.aws.operators.s3.S3DeleteBucketOperator
   airflow.providers.amazon.aws.operators.s3.S3GetBucketTaggingOperator
   airflow.providers.amazon.aws.operators.s3.S3PutBucketTaggingOperator
   airflow.providers.amazon.aws.operators.s3.S3DeleteBucketTaggingOperator
   airflow.providers.amazon.aws.operators.s3.S3CopyObjectOperator
   airflow.providers.amazon.aws.operators.s3.S3CreateObjectOperator
   airflow.providers.amazon.aws.operators.s3.S3DeleteObjectsOperator
   airflow.providers.amazon.aws.operators.s3.S3FileTransformOperator
   airflow.providers.amazon.aws.operators.s3.S3ListOperator
   airflow.providers.amazon.aws.operators.s3.S3ListPrefixesOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.s3.BUCKET_DOES_NOT_EXIST_MSG


.. py:data:: BUCKET_DOES_NOT_EXIST_MSG
   :value: "Bucket with name: %s doesn't exist"



.. py:class:: S3CreateBucketOperator(*, bucket_name, aws_conn_id = 'aws_default', region_name = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator creates an S3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3CreateBucketOperator`

   :param bucket_name: This is bucket name you want to create
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).
   :param region_name: AWS region_name. If not specified fetched from connection.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket_name',)



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3DeleteBucketOperator(bucket_name, force_delete = False, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator deletes an S3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3DeleteBucketOperator`

   :param bucket_name: This is bucket name you want to delete
   :param force_delete: Forcibly delete all objects in the bucket before deleting the bucket
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket_name',)



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3GetBucketTaggingOperator(bucket_name, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator gets tagging from an S3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3GetBucketTaggingOperator`

   :param bucket_name: This is bucket name you want to reference
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket_name',)



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3PutBucketTaggingOperator(bucket_name, key = None, value = None, tag_set = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator puts tagging for an S3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3PutBucketTaggingOperator`

   :param bucket_name: The name of the bucket to add tags to.
   :param key: The key portion of the key/value pair for a tag to be added.
       If a key is provided, a value must be provided as well.
   :param value: The value portion of the key/value pair for a tag to be added.
       If a value is provided, a key must be provided as well.
   :param tag_set: A dictionary containing the tags, or a List of key/value pairs.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then the default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket_name',)



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3DeleteBucketTaggingOperator(bucket_name, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator deletes tagging from an S3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3DeleteBucketTaggingOperator`

   :param bucket_name: This is the name of the bucket to delete tags from.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket_name',)



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3CopyObjectOperator(*, source_bucket_key, dest_bucket_key, source_bucket_name = None, dest_bucket_name = None, source_version_id = None, aws_conn_id = 'aws_default', verify = None, acl_policy = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates a copy of an object that is already stored in S3.

   Note: the S3 connection used here needs to have access to both
   source and destination bucket/key.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3CopyObjectOperator`

   :param source_bucket_key: The key of the source object. (templated)

       It can be either full s3:// style url or relative path from root level.

       When it's specified as a full s3:// url, please omit source_bucket_name.
   :param dest_bucket_key: The key of the object to copy to. (templated)

       The convention to specify `dest_bucket_key` is the same as `source_bucket_key`.
   :param source_bucket_name: Name of the S3 bucket where the source object is in. (templated)

       It should be omitted when `source_bucket_key` is provided as a full s3:// url.
   :param dest_bucket_name: Name of the S3 bucket to where the object is copied. (templated)

       It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
   :param source_version_id: Version ID of the source object (OPTIONAL)
   :param aws_conn_id: Connection id of the S3 connection to use
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.

       You can provide the following values:

       - False: do not validate SSL certificates. SSL will still be used,
                but SSL certificates will not be
                verified.
       - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :param acl_policy: String specifying the canned ACL policy for the file being
       uploaded to the S3 bucket.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('source_bucket_key', 'dest_bucket_key', 'source_bucket_name', 'dest_bucket_name')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3CreateObjectOperator(*, s3_bucket = None, s3_key, data, replace = False, encrypt = False, acl_policy = None, encoding = None, compression = None, aws_conn_id = 'aws_default', verify = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates a new object from `data` as string or bytes.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3CreateObjectOperator`

   :param s3_bucket: Name of the S3 bucket where to save the object. (templated)
       It should be omitted when ``s3_key`` is provided as a full s3:// url.
   :param s3_key: The key of the object to be created. (templated)
       It can be either full s3:// style url or relative path from root level.
       When it's specified as a full s3:// url, please omit ``s3_bucket``.
   :param data: string or bytes to save as content.
   :param replace: If True, it will overwrite the key if it already exists
   :param encrypt: If True, the file will be encrypted on the server-side
       by S3 and will be stored in an encrypted form while at rest in S3.
   :param acl_policy: String specifying the canned ACL policy for the file being
       uploaded to the S3 bucket.
   :param encoding: The string to byte encoding.
       It should be specified only when `data` is provided as string.
   :param compression: Type of compression to use, currently only gzip is supported.
       It can be specified only when `data` is provided as string.
   :param aws_conn_id: Connection id of the S3 connection to use
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.

       You can provide the following values:

       - False: do not validate SSL certificates. SSL will still be used,
                but SSL certificates will not be
                verified.
       - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_bucket', 's3_key', 'data')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3DeleteObjectsOperator(*, bucket, keys = None, prefix = None, aws_conn_id = 'aws_default', verify = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   To enable users to delete single object or multiple objects from a bucket using a single HTTP request.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3DeleteObjectsOperator`

   :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
   :param keys: The key(s) to delete from S3 bucket. (templated)

       When ``keys`` is a string, it's supposed to be the key name of
       the single object to delete.

       When ``keys`` is a list, it's supposed to be the list of the
       keys to delete.

   :param prefix: Prefix of objects to delete. (templated)
       All objects matching this prefix in the bucket will be deleted.
   :param aws_conn_id: Connection id of the S3 connection to use
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.

       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used,
                but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('keys', 'bucket', 'prefix')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3FileTransformOperator(*, source_s3_key, dest_s3_key, transform_script = None, select_expression=None, script_args = None, source_aws_conn_id = 'aws_default', source_verify = None, dest_aws_conn_id = 'aws_default', dest_verify = None, replace = False, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Copies data from a source S3 location to a temporary location on the local filesystem.

   Runs a transformation on this file as specified by the transformation
   script and uploads the output to a destination S3 location.

   The locations of the source and the destination files in the local
   filesystem is provided as a first and second arguments to the
   transformation script. The transformation script is expected to read the
   data from source, transform it and write the output to the local
   destination file. The operator then takes over control and uploads the
   local destination file to S3.

   S3 Select is also available to filter the source contents. Users can
   omit the transformation script if S3 Select expression is specified.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3FileTransformOperator`

   :param source_s3_key: The key to be retrieved from S3. (templated)
   :param dest_s3_key: The key to be written from S3. (templated)
   :param transform_script: location of the executable transformation script
   :param select_expression: S3 Select expression
   :param script_args: arguments for transformation script (templated)
   :param source_aws_conn_id: source s3 connection
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
   :param dest_aws_conn_id: destination s3 connection
   :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
       See: ``source_verify``
   :param replace: Replace dest S3 key if it already exists

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('source_s3_key', 'dest_s3_key', 'script_args')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3ListOperator(*, bucket, prefix = '', delimiter = '', aws_conn_id = 'aws_default', verify = None, apply_wildcard = False, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   List all objects from the bucket with the given string prefix in name.

   This operator returns a python list with the name of objects which can be
   used by `xcom` in the downstream task.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3ListOperator`

   :param bucket: The S3 bucket where to find the objects. (templated)
   :param prefix: Prefix string to filters the objects whose name begin with
       such prefix. (templated)
   :param delimiter: the delimiter marks key hierarchy. (templated)
   :param aws_conn_id: The connection ID to use when connecting to S3 storage.
   :param verify: Whether or not to verify SSL certificates for S3 connection.
   :param apply_wildcard: whether to treat '*' as a wildcard or a plain symbol in the prefix.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.


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

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket', 'prefix', 'delimiter')



   .. py:attribute:: ui_color
      :value: '#ffd700'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: S3ListPrefixesOperator(*, bucket, prefix, delimiter, aws_conn_id = 'aws_default', verify = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   List all subfolders from the bucket with the given string prefix in name.

   This operator returns a python list with the name of all subfolders which
   can be used by `xcom` in the downstream task.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3ListPrefixesOperator`

   :param bucket: The S3 bucket where to find the subfolders. (templated)
   :param prefix: Prefix string to filter the subfolders whose name begin with
       such prefix. (templated)
   :param delimiter: the delimiter marks subfolder hierarchy. (templated)
   :param aws_conn_id: The connection ID to use when connecting to S3 storage.
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.


   **Example**:
       The following operator would list all the subfolders
       from the S3 ``customers/2018/04/`` prefix in the ``data`` bucket. ::

           s3_file = S3ListPrefixesOperator(
               task_id='list_s3_prefixes',
               bucket='data',
               prefix='customers/2018/04/',
               delimiter='/',
               aws_conn_id='aws_customers_conn'
           )

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket', 'prefix', 'delimiter')



   .. py:attribute:: ui_color
      :value: '#ffd700'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
