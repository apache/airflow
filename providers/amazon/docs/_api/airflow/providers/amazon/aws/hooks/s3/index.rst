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

:py:mod:`airflow.providers.amazon.aws.hooks.s3`
===============================================

.. py:module:: airflow.providers.amazon.aws.hooks.s3

.. autoapi-nested-parse::

   Interact with AWS S3, using the boto3 library.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.s3.S3Hook



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.s3.provide_bucket_name
   airflow.providers.amazon.aws.hooks.s3.provide_bucket_name_async
   airflow.providers.amazon.aws.hooks.s3.unify_bucket_name_and_key



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.s3.T
   airflow.providers.amazon.aws.hooks.s3.logger


.. py:data:: T



.. py:data:: logger



.. py:function:: provide_bucket_name(func)

   Provide a bucket name taken from the connection if no bucket name has been passed to the function.


.. py:function:: provide_bucket_name_async(func)

   Provide a bucket name taken from the connection if no bucket name has been passed to the function.


.. py:function:: unify_bucket_name_and_key(func)

   Unify bucket name and key in case no bucket name and at least a key has been passed to the function.


.. py:class:: S3Hook(aws_conn_id = AwsBaseHook.default_conn_name, transfer_config_args = None, extra_args = None, *args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Simple Storage Service (S3).

   Provide thick wrapper around :external+boto3:py:class:`boto3.client("s3") <S3.Client>`
   and :external+boto3:py:class:`boto3.resource("s3") <S3.ServiceResource>`.

   :param transfer_config_args: Configuration object for managed S3 transfers.
   :param extra_args: Extra arguments that may be passed to the download/upload operations.

   .. seealso::
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#s3-transfers

       - For allowed upload extra arguments see ``boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS``.
       - For allowed download extra arguments see ``boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS``.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:property:: extra_args

      Return hook's extra arguments (immutable).


   .. py:method:: parse_s3_url(s3url)
      :staticmethod:

      Parse the S3 Url into a bucket name and key.

      See https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html
      for valid url formats.

      :param s3url: The S3 Url to parse.
      :return: the parsed bucket name and key


   .. py:method:: get_s3_bucket_key(bucket, key, bucket_param_name, key_param_name)
      :staticmethod:

      Get the S3 bucket name and key.

      From either:
      - bucket name and key. Return the info as it is after checking `key` is a relative path.
      - key. Must be a full s3:// url.

      :param bucket: The S3 bucket name
      :param key: The S3 key
      :param bucket_param_name: The parameter name containing the bucket name
      :param key_param_name: The parameter name containing the key name
      :return: the parsed bucket name and key


   .. py:method:: check_for_bucket(bucket_name = None)

      Check if bucket_name exists.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.head_bucket`

      :param bucket_name: the name of the bucket
      :return: True if it exists and False if not.


   .. py:method:: get_bucket(bucket_name = None)

      Return a :py:class:`S3.Bucket` object.

      .. seealso::
          - :external+boto3:py:meth:`S3.ServiceResource.Bucket`

      :param bucket_name: the name of the bucket
      :return: the bucket object to the bucket name.


   .. py:method:: create_bucket(bucket_name = None, region_name = None)

      Create an Amazon S3 bucket.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.create_bucket`

      :param bucket_name: The name of the bucket
      :param region_name: The name of the aws region in which to create the bucket.


   .. py:method:: check_for_prefix(prefix, delimiter, bucket_name = None)

      Check that a prefix exists in a bucket.

      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param delimiter: the delimiter marks key hierarchy.
      :return: False if the prefix does not exist in the bucket and True if it does.


   .. py:method:: list_prefixes(bucket_name = None, prefix = None, delimiter = None, page_size = None, max_items = None)

      List prefixes in a bucket under prefix.

      .. seealso::
          - :external+boto3:py:class:`S3.Paginator.ListObjectsV2`

      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param delimiter: the delimiter marks key hierarchy.
      :param page_size: pagination size
      :param max_items: maximum items to return
      :return: a list of matched prefixes


   .. py:method:: get_head_object_async(client, key, bucket_name = None)
      :async:

      Retrieve metadata of an object.

      :param client: aiobotocore client
      :param bucket_name: Name of the bucket in which the file is stored
      :param key: S3 key that will point to the file


   .. py:method:: list_prefixes_async(client, bucket_name = None, prefix = None, delimiter = None, page_size = None, max_items = None)
      :async:

      List prefixes in a bucket under prefix.

      :param client: ClientCreatorContext
      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param delimiter: the delimiter marks key hierarchy.
      :param page_size: pagination size
      :param max_items: maximum items to return
      :return: a list of matched prefixes


   .. py:method:: get_file_metadata_async(client, bucket_name, key)
      :async:

      Get a list of files that a key matching a wildcard expression exists in a bucket asynchronously.

      :param client: aiobotocore client
      :param bucket_name: the name of the bucket
      :param key: the path to the key


   .. py:method:: check_key_async(client, bucket, bucket_keys, wildcard_match)
      :async:

      Check for all keys in bucket and returns boolean value.

      :param client: aiobotocore client
      :param bucket: the name of the bucket
      :param bucket_keys: S3 keys that will point to the file
      :param wildcard_match: the path to the key


   .. py:method:: check_for_prefix_async(client, prefix, delimiter, bucket_name = None)
      :async:

      Check that a prefix exists in a bucket.

      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param delimiter: the delimiter marks key hierarchy.
      :return: False if the prefix does not exist in the bucket and True if it does.


   .. py:method:: get_files_async(client, bucket, bucket_keys, wildcard_match, delimiter = '/')
      :async:

      Get a list of files in the bucket.


   .. py:method:: is_keys_unchanged_async(client, bucket_name, prefix, inactivity_period = 60 * 60, min_objects = 1, previous_objects = None, inactivity_seconds = 0, allow_delete = True, last_activity_time = None)
      :async:

      Check if new objects have been uploaded and the period has passed; update sensor state accordingly.

      :param client: aiobotocore client
      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param inactivity_period:  the total seconds of inactivity to designate
          keys unchanged. Note, this mechanism is not real time and
          this operator may not return until a poke_interval after this period
          has passed with no additional objects sensed.
      :param min_objects: the minimum number of objects needed for keys unchanged
          sensor to be considered valid.
      :param previous_objects: the set of object ids found during the last poke.
      :param inactivity_seconds: number of inactive seconds
      :param allow_delete: Should this sensor consider objects being deleted
          between pokes valid behavior. If true a warning message will be logged
          when this happens. If false an error will be raised.
      :param last_activity_time: last activity datetime.


   .. py:method:: list_keys(bucket_name = None, prefix = None, delimiter = None, page_size = None, max_items = None, start_after_key = None, from_datetime = None, to_datetime = None, object_filter = None, apply_wildcard = False)

      List keys in a bucket under prefix and not containing delimiter.

      .. seealso::
          - :external+boto3:py:class:`S3.Paginator.ListObjectsV2`

      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param delimiter: the delimiter marks key hierarchy.
      :param page_size: pagination size
      :param max_items: maximum items to return
      :param start_after_key: should return only keys greater than this key
      :param from_datetime: should return only keys with LastModified attr greater than this equal
          from_datetime
      :param to_datetime: should return only keys with LastModified attr less than this to_datetime
      :param object_filter: Function that receives the list of the S3 objects, from_datetime and
          to_datetime and returns the List of matched key.
      :param apply_wildcard: whether to treat '*' as a wildcard or a plain symbol in the prefix.

      **Example**: Returns the list of S3 object with LastModified attr greater than from_datetime
           and less than to_datetime:

      .. code-block:: python

          def object_filter(
              keys: list,
              from_datetime: datetime | None = None,
              to_datetime: datetime | None = None,
          ) -> list:
              def _is_in_period(input_date: datetime) -> bool:
                  if from_datetime is not None and input_date < from_datetime:
                      return False

                  if to_datetime is not None and input_date > to_datetime:
                      return False
                  return True

              return [k["Key"] for k in keys if _is_in_period(k["LastModified"])]

      :return: a list of matched keys


   .. py:method:: get_file_metadata(prefix, bucket_name = None, page_size = None, max_items = None)

      List metadata objects in a bucket under prefix.

      .. seealso::
          - :external+boto3:py:class:`S3.Paginator.ListObjectsV2`

      :param prefix: a key prefix
      :param bucket_name: the name of the bucket
      :param page_size: pagination size
      :param max_items: maximum items to return
      :return: a list of metadata of objects


   .. py:method:: head_object(key, bucket_name = None)

      Retrieve metadata of an object.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.head_object`

      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which the file is stored
      :return: metadata of an object


   .. py:method:: check_for_key(key, bucket_name = None)

      Check if a key exists in a bucket.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.head_object`

      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which the file is stored
      :return: True if the key exists and False if not.


   .. py:method:: get_key(key, bucket_name = None)

      Return a :py:class:`S3.Object`.

      .. seealso::
          - :external+boto3:py:meth:`S3.ServiceResource.Object`

      :param key: the path to the key
      :param bucket_name: the name of the bucket
      :return: the key object from the bucket


   .. py:method:: read_key(key, bucket_name = None)

      Read a key from S3.

      .. seealso::
          - :external+boto3:py:meth:`S3.Object.get`

      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which the file is stored
      :return: the content of the key


   .. py:method:: select_key(key, bucket_name = None, expression = None, expression_type = None, input_serialization = None, output_serialization = None)

      Read a key with S3 Select.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.select_object_content`

      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which the file is stored
      :param expression: S3 Select expression
      :param expression_type: S3 Select expression type
      :param input_serialization: S3 Select input data serialization format
      :param output_serialization: S3 Select output data serialization format
      :return: retrieved subset of original data by S3 Select


   .. py:method:: check_for_wildcard_key(wildcard_key, bucket_name = None, delimiter = '')

      Check that a key matching a wildcard expression exists in a bucket.

      :param wildcard_key: the path to the key
      :param bucket_name: the name of the bucket
      :param delimiter: the delimiter marks key hierarchy
      :return: True if a key exists and False if not.


   .. py:method:: get_wildcard_key(wildcard_key, bucket_name = None, delimiter = '')

      Return a boto3.s3.Object object matching the wildcard expression.

      :param wildcard_key: the path to the key
      :param bucket_name: the name of the bucket
      :param delimiter: the delimiter marks key hierarchy
      :return: the key object from the bucket or None if none has been found.


   .. py:method:: load_file(filename, key, bucket_name = None, replace = False, encrypt = False, gzip = False, acl_policy = None)

      Load a local file to S3.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.upload_file`

      :param filename: path to the file to load.
      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which to store the file
      :param replace: A flag to decide whether or not to overwrite the key
          if it already exists. If replace is False and the key exists, an
          error will be raised.
      :param encrypt: If True, the file will be encrypted on the server-side
          by S3 and will be stored in an encrypted form while at rest in S3.
      :param gzip: If True, the file will be compressed locally
      :param acl_policy: String specifying the canned ACL policy for the file being
          uploaded to the S3 bucket.


   .. py:method:: load_string(string_data, key, bucket_name = None, replace = False, encrypt = False, encoding = None, acl_policy = None, compression = None)

      Load a string to S3.

      This is provided as a convenience to drop a string in S3. It uses the
      boto infrastructure to ship a file to s3.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.upload_fileobj`

      :param string_data: str to set as content for the key.
      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which to store the file
      :param replace: A flag to decide whether or not to overwrite the key
          if it already exists
      :param encrypt: If True, the file will be encrypted on the server-side
          by S3 and will be stored in an encrypted form while at rest in S3.
      :param encoding: The string to byte encoding
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be uploaded
      :param compression: Type of compression to use, currently only gzip is supported.


   .. py:method:: load_bytes(bytes_data, key, bucket_name = None, replace = False, encrypt = False, acl_policy = None)

      Load bytes to S3.

      This is provided as a convenience to drop bytes data into S3. It uses the
      boto infrastructure to ship a file to s3.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.upload_fileobj`

      :param bytes_data: bytes to set as content for the key.
      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which to store the file
      :param replace: A flag to decide whether or not to overwrite the key
          if it already exists
      :param encrypt: If True, the file will be encrypted on the server-side
          by S3 and will be stored in an encrypted form while at rest in S3.
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be uploaded


   .. py:method:: load_file_obj(file_obj, key, bucket_name = None, replace = False, encrypt = False, acl_policy = None)

      Load a file object to S3.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.upload_fileobj`

      :param file_obj: The file-like object to set as the content for the S3 key.
      :param key: S3 key that will point to the file
      :param bucket_name: Name of the bucket in which to store the file
      :param replace: A flag that indicates whether to overwrite the key
          if it already exists.
      :param encrypt: If True, S3 encrypts the file on the server,
          and the file is stored in encrypted form at rest in S3.
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be uploaded


   .. py:method:: copy_object(source_bucket_key, dest_bucket_key, source_bucket_name = None, dest_bucket_name = None, source_version_id = None, acl_policy = None)

      Create a copy of an object that is already stored in S3.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.copy_object`

      Note: the S3 connection used here needs to have access to both
      source and destination bucket/key.

      :param source_bucket_key: The key of the source object.

          It can be either full s3:// style url or relative path from root level.

          When it's specified as a full s3:// url, please omit source_bucket_name.
      :param dest_bucket_key: The key of the object to copy to.

          The convention to specify `dest_bucket_key` is the same
          as `source_bucket_key`.
      :param source_bucket_name: Name of the S3 bucket where the source object is in.

          It should be omitted when `source_bucket_key` is provided as a full s3:// url.
      :param dest_bucket_name: Name of the S3 bucket to where the object is copied.

          It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
      :param source_version_id: Version ID of the source object (OPTIONAL)
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be copied which is private by default.


   .. py:method:: delete_bucket(bucket_name, force_delete = False, max_retries = 5)

      To delete s3 bucket, delete all s3 bucket objects and then delete the bucket.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.delete_bucket`

      :param bucket_name: Bucket name
      :param force_delete: Enable this to delete bucket even if not empty
      :param max_retries: A bucket must be empty to be deleted.  If force_delete is true,
          then retries may help prevent a race condition between deleting objects in the
          bucket and trying to delete the bucket.
      :return: None


   .. py:method:: delete_objects(bucket, keys)

      Delete keys from the bucket.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.delete_objects`

      :param bucket: Name of the bucket in which you are going to delete object(s)
      :param keys: The key(s) to delete from S3 bucket.

          When ``keys`` is a string, it's supposed to be the key name of
          the single object to delete.

          When ``keys`` is a list, it's supposed to be the list of the
          keys to delete.


   .. py:method:: download_file(key, bucket_name = None, local_path = None, preserve_file_name = False, use_autogenerated_subdir = True)

      Download a file from the S3 location to the local file system.

      .. seealso::
          - :external+boto3:py:meth:`S3.Object.download_fileobj`

      :param key: The key path in S3.
      :param bucket_name: The specific bucket to use.
      :param local_path: The local path to the downloaded file. If no path is provided it will use the
          system's temporary directory.
      :param preserve_file_name: If you want the downloaded file name to be the same name as it is in S3,
          set this parameter to True. When set to False, a random filename will be generated.
          Default: False.
      :param use_autogenerated_subdir: Pairs with 'preserve_file_name = True' to download the file into a
          random generated folder inside the 'local_path', useful to avoid collisions between various tasks
          that might download the same file name. Set it to 'False' if you don't want it, and you want a
          predictable path.
          Default: True.
      :return: the file name.


   .. py:method:: generate_presigned_url(client_method, params = None, expires_in = 3600, http_method = None)

      Generate a presigned url given a client, its method, and arguments.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.generate_presigned_url`

      :param client_method: The client method to presign for.
      :param params: The parameters normally passed to ClientMethod.
      :param expires_in: The number of seconds the presigned url is valid for.
          By default it expires in an hour (3600 seconds).
      :param http_method: The http method to use on the generated url.
          By default, the http method is whatever is used in the method's model.
      :return: The presigned url.


   .. py:method:: get_bucket_tagging(bucket_name = None)

      Get a List of tags from a bucket.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.get_bucket_tagging`

      :param bucket_name: The name of the bucket.
      :return: A List containing the key/value pairs for the tags


   .. py:method:: put_bucket_tagging(tag_set = None, key = None, value = None, bucket_name = None)

      Overwrite the existing TagSet with provided tags; must provide a TagSet, a key/value pair, or both.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.put_bucket_tagging`

      :param tag_set: A dictionary containing the key/value pairs for the tags,
          or a list already formatted for the API
      :param key: The Key for the new TagSet entry.
      :param value: The Value for the new TagSet entry.
      :param bucket_name: The name of the bucket.

      :return: None


   .. py:method:: delete_bucket_tagging(bucket_name = None)

      Delete all tags from a bucket.

      .. seealso::
          - :external+boto3:py:meth:`S3.Client.delete_bucket_tagging`

      :param bucket_name: The name of the bucket.
      :return: None
