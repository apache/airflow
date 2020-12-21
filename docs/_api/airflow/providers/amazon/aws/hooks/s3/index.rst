:mod:`airflow.providers.amazon.aws.hooks.s3`
============================================

.. py:module:: airflow.providers.amazon.aws.hooks.s3

.. autoapi-nested-parse::

   Interact with AWS S3, using the boto3 library.



Module Contents
---------------

.. data:: T
   

   

.. function:: provide_bucket_name(func: T) -> T
   Function decorator that provides a bucket name taken from the connection
   in case no bucket name has been passed to the function.


.. function:: unify_bucket_name_and_key(func: T) -> T
   Function decorator that unifies bucket name and key taken from the key
   in case no bucket name and at least a key has been passed to the function.


.. py:class:: S3Hook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS S3, using the boto3 library.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. staticmethod:: parse_s3_url(s3url: str)

      Parses the S3 Url into a bucket name and key.

      :param s3url: The S3 Url to parse.
      :rtype s3url: str
      :return: the parsed bucket name and key
      :rtype: tuple of str



   
   .. method:: check_for_bucket(self, bucket_name: Optional[str] = None)

      Check if bucket_name exists.

      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :return: True if it exists and False if not.
      :rtype: bool



   
   .. method:: get_bucket(self, bucket_name: Optional[str] = None)

      Returns a boto3.S3.Bucket object

      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :return: the bucket object to the bucket name.
      :rtype: boto3.S3.Bucket



   
   .. method:: create_bucket(self, bucket_name: Optional[str] = None, region_name: Optional[str] = None)

      Creates an Amazon S3 bucket.

      :param bucket_name: The name of the bucket
      :type bucket_name: str
      :param region_name: The name of the aws region in which to create the bucket.
      :type region_name: str



   
   .. method:: check_for_prefix(self, prefix: str, delimiter: str, bucket_name: Optional[str] = None)

      Checks that a prefix exists in a bucket

      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :param prefix: a key prefix
      :type prefix: str
      :param delimiter: the delimiter marks key hierarchy.
      :type delimiter: str
      :return: False if the prefix does not exist in the bucket and True if it does.
      :rtype: bool



   
   .. method:: list_prefixes(self, bucket_name: Optional[str] = None, prefix: Optional[str] = None, delimiter: Optional[str] = None, page_size: Optional[int] = None, max_items: Optional[int] = None)

      Lists prefixes in a bucket under prefix

      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :param prefix: a key prefix
      :type prefix: str
      :param delimiter: the delimiter marks key hierarchy.
      :type delimiter: str
      :param page_size: pagination size
      :type page_size: int
      :param max_items: maximum items to return
      :type max_items: int
      :return: a list of matched prefixes
      :rtype: list



   
   .. method:: list_keys(self, bucket_name: Optional[str] = None, prefix: Optional[str] = None, delimiter: Optional[str] = None, page_size: Optional[int] = None, max_items: Optional[int] = None)

      Lists keys in a bucket under prefix and not containing delimiter

      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :param prefix: a key prefix
      :type prefix: str
      :param delimiter: the delimiter marks key hierarchy.
      :type delimiter: str
      :param page_size: pagination size
      :type page_size: int
      :param max_items: maximum items to return
      :type max_items: int
      :return: a list of matched keys
      :rtype: list



   
   .. method:: check_for_key(self, key: str, bucket_name: Optional[str] = None)

      Checks if a key exists in a bucket

      :param key: S3 key that will point to the file
      :type key: str
      :param bucket_name: Name of the bucket in which the file is stored
      :type bucket_name: str
      :return: True if the key exists and False if not.
      :rtype: bool



   
   .. method:: get_key(self, key: str, bucket_name: Optional[str] = None)

      Returns a boto3.s3.Object

      :param key: the path to the key
      :type key: str
      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :return: the key object from the bucket
      :rtype: boto3.s3.Object



   
   .. method:: read_key(self, key: str, bucket_name: Optional[str] = None)

      Reads a key from S3

      :param key: S3 key that will point to the file
      :type key: str
      :param bucket_name: Name of the bucket in which the file is stored
      :type bucket_name: str
      :return: the content of the key
      :rtype: str



   
   .. method:: select_key(self, key: str, bucket_name: Optional[str] = None, expression: Optional[str] = None, expression_type: Optional[str] = None, input_serialization: Optional[Dict[str, Any]] = None, output_serialization: Optional[Dict[str, Any]] = None)

      Reads a key with S3 Select.

      :param key: S3 key that will point to the file
      :type key: str
      :param bucket_name: Name of the bucket in which the file is stored
      :type bucket_name: str
      :param expression: S3 Select expression
      :type expression: str
      :param expression_type: S3 Select expression type
      :type expression_type: str
      :param input_serialization: S3 Select input data serialization format
      :type input_serialization: dict
      :param output_serialization: S3 Select output data serialization format
      :type output_serialization: dict
      :return: retrieved subset of original data by S3 Select
      :rtype: str

      .. seealso::
          For more details about S3 Select parameters:
          http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.select_object_content



   
   .. method:: check_for_wildcard_key(self, wildcard_key: str, bucket_name: Optional[str] = None, delimiter: str = '')

      Checks that a key matching a wildcard expression exists in a bucket

      :param wildcard_key: the path to the key
      :type wildcard_key: str
      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :param delimiter: the delimiter marks key hierarchy
      :type delimiter: str
      :return: True if a key exists and False if not.
      :rtype: bool



   
   .. method:: get_wildcard_key(self, wildcard_key: str, bucket_name: Optional[str] = None, delimiter: str = '')

      Returns a boto3.s3.Object object matching the wildcard expression

      :param wildcard_key: the path to the key
      :type wildcard_key: str
      :param bucket_name: the name of the bucket
      :type bucket_name: str
      :param delimiter: the delimiter marks key hierarchy
      :type delimiter: str
      :return: the key object from the bucket or None if none has been found.
      :rtype: boto3.s3.Object



   
   .. method:: load_file(self, filename: str, key: str, bucket_name: Optional[str] = None, replace: bool = False, encrypt: bool = False, gzip: bool = False, acl_policy: Optional[str] = None)

      Loads a local file to S3

      :param filename: name of the file to load.
      :type filename: str
      :param key: S3 key that will point to the file
      :type key: str
      :param bucket_name: Name of the bucket in which to store the file
      :type bucket_name: str
      :param replace: A flag to decide whether or not to overwrite the key
          if it already exists. If replace is False and the key exists, an
          error will be raised.
      :type replace: bool
      :param encrypt: If True, the file will be encrypted on the server-side
          by S3 and will be stored in an encrypted form while at rest in S3.
      :type encrypt: bool
      :param gzip: If True, the file will be compressed locally
      :type gzip: bool
      :param acl_policy: String specifying the canned ACL policy for the file being
          uploaded to the S3 bucket.
      :type acl_policy: str



   
   .. method:: load_string(self, string_data: str, key: str, bucket_name: Optional[str] = None, replace: bool = False, encrypt: bool = False, encoding: Optional[str] = None, acl_policy: Optional[str] = None)

      Loads a string to S3

      This is provided as a convenience to drop a string in S3. It uses the
      boto infrastructure to ship a file to s3.

      :param string_data: str to set as content for the key.
      :type string_data: str
      :param key: S3 key that will point to the file
      :type key: str
      :param bucket_name: Name of the bucket in which to store the file
      :type bucket_name: str
      :param replace: A flag to decide whether or not to overwrite the key
          if it already exists
      :type replace: bool
      :param encrypt: If True, the file will be encrypted on the server-side
          by S3 and will be stored in an encrypted form while at rest in S3.
      :type encrypt: bool
      :param encoding: The string to byte encoding
      :type encoding: str
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be uploaded
      :type acl_policy: str



   
   .. method:: load_bytes(self, bytes_data: bytes, key: str, bucket_name: Optional[str] = None, replace: bool = False, encrypt: bool = False, acl_policy: Optional[str] = None)

      Loads bytes to S3

      This is provided as a convenience to drop a string in S3. It uses the
      boto infrastructure to ship a file to s3.

      :param bytes_data: bytes to set as content for the key.
      :type bytes_data: bytes
      :param key: S3 key that will point to the file
      :type key: str
      :param bucket_name: Name of the bucket in which to store the file
      :type bucket_name: str
      :param replace: A flag to decide whether or not to overwrite the key
          if it already exists
      :type replace: bool
      :param encrypt: If True, the file will be encrypted on the server-side
          by S3 and will be stored in an encrypted form while at rest in S3.
      :type encrypt: bool
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be uploaded
      :type acl_policy: str



   
   .. method:: load_file_obj(self, file_obj: BytesIO, key: str, bucket_name: Optional[str] = None, replace: bool = False, encrypt: bool = False, acl_policy: Optional[str] = None)

      Loads a file object to S3

      :param file_obj: The file-like object to set as the content for the S3 key.
      :type file_obj: file-like object
      :param key: S3 key that will point to the file
      :type key: str
      :param bucket_name: Name of the bucket in which to store the file
      :type bucket_name: str
      :param replace: A flag that indicates whether to overwrite the key
          if it already exists.
      :type replace: bool
      :param encrypt: If True, S3 encrypts the file on the server,
          and the file is stored in encrypted form at rest in S3.
      :type encrypt: bool
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be uploaded
      :type acl_policy: str



   
   .. method:: _upload_file_obj(self, file_obj: BytesIO, key: str, bucket_name: Optional[str] = None, replace: bool = False, encrypt: bool = False, acl_policy: Optional[str] = None)



   
   .. method:: copy_object(self, source_bucket_key: str, dest_bucket_key: str, source_bucket_name: Optional[str] = None, dest_bucket_name: Optional[str] = None, source_version_id: Optional[str] = None, acl_policy: Optional[str] = None)

      Creates a copy of an object that is already stored in S3.

      Note: the S3 connection used here needs to have access to both
      source and destination bucket/key.

      :param source_bucket_key: The key of the source object.

          It can be either full s3:// style url or relative path from root level.

          When it's specified as a full s3:// url, please omit source_bucket_name.
      :type source_bucket_key: str
      :param dest_bucket_key: The key of the object to copy to.

          The convention to specify `dest_bucket_key` is the same
          as `source_bucket_key`.
      :type dest_bucket_key: str
      :param source_bucket_name: Name of the S3 bucket where the source object is in.

          It should be omitted when `source_bucket_key` is provided as a full s3:// url.
      :type source_bucket_name: str
      :param dest_bucket_name: Name of the S3 bucket to where the object is copied.

          It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
      :type dest_bucket_name: str
      :param source_version_id: Version ID of the source object (OPTIONAL)
      :type source_version_id: str
      :param acl_policy: The string to specify the canned ACL policy for the
          object to be copied which is private by default.
      :type acl_policy: str



   
   .. method:: delete_bucket(self, bucket_name: str, force_delete: bool = False)

      To delete s3 bucket, delete all s3 bucket objects and then delete the bucket.

      :param bucket_name: Bucket name
      :type bucket_name: str
      :param force_delete: Enable this to delete bucket even if not empty
      :type force_delete: bool
      :return: None
      :rtype: None



   
   .. method:: delete_objects(self, bucket: str, keys: Union[str, list])

      Delete keys from the bucket.

      :param bucket: Name of the bucket in which you are going to delete object(s)
      :type bucket: str
      :param keys: The key(s) to delete from S3 bucket.

          When ``keys`` is a string, it's supposed to be the key name of
          the single object to delete.

          When ``keys`` is a list, it's supposed to be the list of the
          keys to delete.
      :type keys: str or list



   
   .. method:: download_file(self, key: str, bucket_name: Optional[str] = None, local_path: Optional[str] = None)

      Downloads a file from the S3 location to the local file system.

      :param key: The key path in S3.
      :type key: str
      :param bucket_name: The specific bucket to use.
      :type bucket_name: Optional[str]
      :param local_path: The local path to the downloaded file. If no path is provided it will use the
          system's temporary directory.
      :type local_path: Optional[str]
      :return: the file name.
      :rtype: str



   
   .. method:: generate_presigned_url(self, client_method: str, params: Optional[dict] = None, expires_in: int = 3600, http_method: Optional[str] = None)

      Generate a presigned url given a client, its method, and arguments

      :param client_method: The client method to presign for.
      :type client_method: str
      :param params: The parameters normally passed to ClientMethod.
      :type params: dict
      :param expires_in: The number of seconds the presigned url is valid for.
          By default it expires in an hour (3600 seconds).
      :type expires_in: int
      :param http_method: The http method to use on the generated url.
          By default, the http method is whatever is used in the method's model.
      :type http_method: str
      :return: The presigned url.
      :rtype: str




