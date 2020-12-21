:mod:`airflow.providers.google.cloud.hooks.gcs`
===============================================

.. py:module:: airflow.providers.google.cloud.hooks.gcs

.. autoapi-nested-parse::

   This module contains a Google Cloud Storage hook.



Module Contents
---------------

.. data:: RT
   

   

.. data:: T
   

   

.. function:: _fallback_object_url_to_object_name_and_bucket_name(object_url_keyword_arg_name='object_url', bucket_name_keyword_arg_name='bucket_name', object_name_keyword_arg_name='object_name') -> Callable[[T], T]
   Decorator factory that convert object URL parameter to object name and bucket name parameter.

   :param object_url_keyword_arg_name: Name of the object URL parameter
   :type object_url_keyword_arg_name: str
   :param bucket_name_keyword_arg_name: Name of the bucket name parameter
   :type bucket_name_keyword_arg_name: str
   :param object_name_keyword_arg_name: Name of the object name parameter
   :type object_name_keyword_arg_name: str
   :return: Decorator


.. py:class:: GCSHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, google_cloud_storage_conn_id: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Interact with Google Cloud Storage. This hook uses the Google Cloud
   connection.

   .. attribute:: _conn
      :annotation: :Optional[storage.Client]

      

   
   .. method:: get_conn(self)

      Returns a Google Cloud Storage service object.



   
   .. method:: copy(self, source_bucket: str, source_object: str, destination_bucket: Optional[str] = None, destination_object: Optional[str] = None)

      Copies an object from a bucket to another, with renaming if requested.

      destination_bucket or destination_object can be omitted, in which case
      source bucket/object is used, but not both.

      :param source_bucket: The bucket of the object to copy from.
      :type source_bucket: str
      :param source_object: The object to copy.
      :type source_object: str
      :param destination_bucket: The destination of the object to copied to.
          Can be omitted; then the same bucket is used.
      :type destination_bucket: str
      :param destination_object: The (renamed) path of the object if given.
          Can be omitted; then the same name is used.
      :type destination_object: str



   
   .. method:: rewrite(self, source_bucket: str, source_object: str, destination_bucket: str, destination_object: Optional[str] = None)

      Has the same functionality as copy, except that will work on files
      over 5 TB, as well as when copying between locations and/or storage
      classes.

      destination_object can be omitted, in which case source_object is used.

      :param source_bucket: The bucket of the object to copy from.
      :type source_bucket: str
      :param source_object: The object to copy.
      :type source_object: str
      :param destination_bucket: The destination of the object to copied to.
      :type destination_bucket: str
      :param destination_object: The (renamed) path of the object if given.
          Can be omitted; then the same name is used.
      :type destination_object: str



   
   .. method:: download(self, object_name: str, bucket_name: Optional[str], filename: Optional[str] = None)

      Downloads a file from Google Cloud Storage.

      When no filename is supplied, the operator loads the file into memory and returns its
      content. When a filename is supplied, it writes the file to the specified location and
      returns the location. For file sizes that exceed the available memory it is recommended
      to write to a file.

      :param bucket_name: The bucket to fetch from.
      :type bucket_name: str
      :param object_name: The object to fetch.
      :type object_name: str
      :param filename: If set, a local file path where the file should be written to.
      :type filename: str



   
   .. method:: provide_file(self, bucket_name: Optional[str] = None, object_name: Optional[str] = None, object_url: Optional[str] = None)

      Downloads the file to a temporary directory and returns a file handle

      You can use this method by passing the bucket_name and object_name parameters
      or just object_url parameter.

      :param bucket_name: The bucket to fetch from.
      :type bucket_name: str
      :param object_name: The object to fetch.
      :type object_name: str
      :param object_url: File reference url. Must start with "gs: //"
      :type object_url: str
      :return: File handler



   
   .. method:: provide_file_and_upload(self, bucket_name: Optional[str] = None, object_name: Optional[str] = None, object_url: Optional[str] = None)

      Creates temporary file, returns a file handle and uploads the files content
      on close.

      You can use this method by passing the bucket_name and object_name parameters
      or just object_url parameter.

      :param bucket_name: The bucket to fetch from.
      :type bucket_name: str
      :param object_name: The object to fetch.
      :type object_name: str
      :param object_url: File reference url. Must start with "gs: //"
      :type object_url: str
      :return: File handler



   
   .. method:: upload(self, bucket_name: str, object_name: str, filename: Optional[str] = None, data: Optional[Union[str, bytes]] = None, mime_type: Optional[str] = None, gzip: bool = False, encoding: str = 'utf-8')

      Uploads a local file or file data as string or bytes to Google Cloud Storage.

      :param bucket_name: The bucket to upload to.
      :type bucket_name: str
      :param object_name: The object name to set when uploading the file.
      :type object_name: str
      :param filename: The local file path to the file to be uploaded.
      :type filename: str
      :param data: The file's data as a string or bytes to be uploaded.
      :type data: str
      :param mime_type: The file's mime type set when uploading the file.
      :type mime_type: str
      :param gzip: Option to compress local file or file data for upload
      :type gzip: bool
      :param encoding: bytes encoding for file data if provided as string
      :type encoding: str



   
   .. method:: exists(self, bucket_name: str, object_name: str)

      Checks for the existence of a file in Google Cloud Storage.

      :param bucket_name: The Google Cloud Storage bucket where the object is.
      :type bucket_name: str
      :param object_name: The name of the blob_name to check in the Google cloud
          storage bucket.
      :type object_name: str



   
   .. method:: get_blob_update_time(self, bucket_name: str, object_name: str)

      Get the update time of a file in Google Cloud Storage

      :param bucket_name: The Google Cloud Storage bucket where the object is.
      :type bucket_name: str
      :param object_name: The name of the blob to get updated time from the Google cloud
          storage bucket.
      :type object_name: str



   
   .. method:: is_updated_after(self, bucket_name: str, object_name: str, ts: datetime)

      Checks if an blob_name is updated in Google Cloud Storage.

      :param bucket_name: The Google Cloud Storage bucket where the object is.
      :type bucket_name: str
      :param object_name: The name of the object to check in the Google cloud
          storage bucket.
      :type object_name: str
      :param ts: The timestamp to check against.
      :type ts: datetime.datetime



   
   .. method:: is_updated_between(self, bucket_name: str, object_name: str, min_ts: datetime, max_ts: datetime)

      Checks if an blob_name is updated in Google Cloud Storage.

      :param bucket_name: The Google Cloud Storage bucket where the object is.
      :type bucket_name: str
      :param object_name: The name of the object to check in the Google cloud
              storage bucket.
      :type object_name: str
      :param min_ts: The minimum timestamp to check against.
      :type min_ts: datetime.datetime
      :param max_ts: The maximum timestamp to check against.
      :type max_ts: datetime.datetime



   
   .. method:: is_updated_before(self, bucket_name: str, object_name: str, ts: datetime)

      Checks if an blob_name is updated before given time in Google Cloud Storage.

      :param bucket_name: The Google Cloud Storage bucket where the object is.
      :type bucket_name: str
      :param object_name: The name of the object to check in the Google cloud
          storage bucket.
      :type object_name: str
      :param ts: The timestamp to check against.
      :type ts: datetime.datetime



   
   .. method:: is_older_than(self, bucket_name: str, object_name: str, seconds: int)

      Check if object is older than given time

      :param bucket_name: The Google Cloud Storage bucket where the object is.
      :type bucket_name: str
      :param object_name: The name of the object to check in the Google cloud
          storage bucket.
      :type object_name: str
      :param seconds: The time in seconds to check against
      :type seconds: int



   
   .. method:: delete(self, bucket_name: str, object_name: str)

      Deletes an object from the bucket.

      :param bucket_name: name of the bucket, where the object resides
      :type bucket_name: str
      :param object_name: name of the object to delete
      :type object_name: str



   
   .. method:: delete_bucket(self, bucket_name: str, force: bool = False)

      Delete a bucket object from the Google Cloud Storage.

      :param bucket_name: name of the bucket which will be deleted
      :type bucket_name: str
      :param force: false not allow to delete non empty bucket, set force=True
          allows to delete non empty bucket
      :type: bool



   
   .. method:: list(self, bucket_name, versions=None, max_results=None, prefix=None, delimiter=None)

      List all objects from the bucket with the give string prefix in name

      :param bucket_name: bucket name
      :type bucket_name: str
      :param versions: if true, list all versions of the objects
      :type versions: bool
      :param max_results: max count of items to return in a single page of responses
      :type max_results: int
      :param prefix: prefix string which filters objects whose name begin with
          this prefix
      :type prefix: str
      :param delimiter: filters objects based on the delimiter (for e.g '.csv')
      :type delimiter: str
      :return: a stream of object names matching the filtering criteria



   
   .. method:: get_size(self, bucket_name: str, object_name: str)

      Gets the size of a file in Google Cloud Storage.

      :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
      :type bucket_name: str
      :param object_name: The name of the object to check in the Google
          cloud storage bucket_name.
      :type object_name: str



   
   .. method:: get_crc32c(self, bucket_name: str, object_name: str)

      Gets the CRC32c checksum of an object in Google Cloud Storage.

      :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
      :type bucket_name: str
      :param object_name: The name of the object to check in the Google cloud
          storage bucket_name.
      :type object_name: str



   
   .. method:: get_md5hash(self, bucket_name: str, object_name: str)

      Gets the MD5 hash of an object in Google Cloud Storage.

      :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
      :type bucket_name: str
      :param object_name: The name of the object to check in the Google cloud
          storage bucket_name.
      :type object_name: str



   
   .. method:: create_bucket(self, bucket_name: str, resource: Optional[dict] = None, storage_class: str = 'MULTI_REGIONAL', location: str = 'US', project_id: Optional[str] = None, labels: Optional[dict] = None)

      Creates a new bucket. Google Cloud Storage uses a flat namespace, so
      you can't create a bucket with a name that is already in use.

      .. seealso::
          For more information, see Bucket Naming Guidelines:
          https://cloud.google.com/storage/docs/bucketnaming.html#requirements

      :param bucket_name: The name of the bucket.
      :type bucket_name: str
      :param resource: An optional dict with parameters for creating the bucket.
          For information on available parameters, see Cloud Storage API doc:
          https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
      :type resource: dict
      :param storage_class: This defines how objects in the bucket are stored
          and determines the SLA and the cost of storage. Values include

          - ``MULTI_REGIONAL``
          - ``REGIONAL``
          - ``STANDARD``
          - ``NEARLINE``
          - ``COLDLINE``.

          If this value is not specified when the bucket is
          created, it will default to STANDARD.
      :type storage_class: str
      :param location: The location of the bucket.
          Object data for objects in the bucket resides in physical storage
          within this region. Defaults to US.

          .. seealso::
              https://developers.google.com/storage/docs/bucket-locations

      :type location: str
      :param project_id: The ID of the Google Cloud Project.
      :type project_id: str
      :param labels: User-provided labels, in key/value pairs.
      :type labels: dict
      :return: If successful, it returns the ``id`` of the bucket.



   
   .. method:: insert_bucket_acl(self, bucket_name: str, entity: str, role: str, user_project: Optional[str] = None)

      Creates a new ACL entry on the specified bucket_name.
      See: https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls/insert

      :param bucket_name: Name of a bucket_name.
      :type bucket_name: str
      :param entity: The entity holding the permission, in one of the following forms:
          user-userId, user-email, group-groupId, group-email, domain-domain,
          project-team-projectId, allUsers, allAuthenticatedUsers.
          See: https://cloud.google.com/storage/docs/access-control/lists#scopes
      :type entity: str
      :param role: The access permission for the entity.
          Acceptable values are: "OWNER", "READER", "WRITER".
      :type role: str
      :param user_project: (Optional) The project to be billed for this request.
          Required for Requester Pays buckets.
      :type user_project: str



   
   .. method:: insert_object_acl(self, bucket_name: str, object_name: str, entity: str, role: str, generation: Optional[int] = None, user_project: Optional[str] = None)

      Creates a new ACL entry on the specified object.
      See: https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert

      :param bucket_name: Name of a bucket_name.
      :type bucket_name: str
      :param object_name: Name of the object. For information about how to URL encode
          object names to be path safe, see:
          https://cloud.google.com/storage/docs/json_api/#encoding
      :type object_name: str
      :param entity: The entity holding the permission, in one of the following forms:
          user-userId, user-email, group-groupId, group-email, domain-domain,
          project-team-projectId, allUsers, allAuthenticatedUsers
          See: https://cloud.google.com/storage/docs/access-control/lists#scopes
      :type entity: str
      :param role: The access permission for the entity.
          Acceptable values are: "OWNER", "READER".
      :type role: str
      :param generation: Optional. If present, selects a specific revision of this object.
      :type generation: long
      :param user_project: (Optional) The project to be billed for this request.
          Required for Requester Pays buckets.
      :type user_project: str



   
   .. method:: compose(self, bucket_name: str, source_objects: List, destination_object: str)

      Composes a list of existing object into a new object in the same storage bucket_name

      Currently it only supports up to 32 objects that can be concatenated
      in a single operation

      https://cloud.google.com/storage/docs/json_api/v1/objects/compose

      :param bucket_name: The name of the bucket containing the source objects.
          This is also the same bucket to store the composed destination object.
      :type bucket_name: str
      :param source_objects: The list of source objects that will be composed
          into a single object.
      :type source_objects: list
      :param destination_object: The path of the object if given.
      :type destination_object: str



   
   .. method:: sync(self, source_bucket: str, destination_bucket: str, source_object: Optional[str] = None, destination_object: Optional[str] = None, recursive: bool = True, allow_overwrite: bool = False, delete_extra_files: bool = False)

      Synchronizes the contents of the buckets.

      Parameters ``source_object`` and ``destination_object`` describe the root sync directories. If they
      are not passed, the entire bucket will be synchronized. If they are passed, they should point
      to directories.

      .. note::
          The synchronization of individual files is not supported. Only entire directories can be
          synchronized.

      :param source_bucket: The name of the bucket containing the source objects.
      :type source_bucket: str
      :param destination_bucket: The name of the bucket containing the destination objects.
      :type destination_bucket: str
      :param source_object: The root sync directory in the source bucket.
      :type source_object: Optional[str]
      :param destination_object: The root sync directory in the destination bucket.
      :type destination_object: Optional[str]
      :param recursive: If True, subdirectories will be considered
      :type recursive: bool
      :param recursive: If True, subdirectories will be considered
      :type recursive: bool
      :param allow_overwrite: if True, the files will be overwritten if a mismatched file is found.
          By default, overwriting files is not allowed
      :type allow_overwrite: bool
      :param delete_extra_files: if True, deletes additional files from the source that not found in the
          destination. By default extra files are not deleted.

          .. note::
              This option can delete data quickly if you specify the wrong source/destination combination.

      :type delete_extra_files: bool
      :return: none



   
   .. method:: _calculate_sync_destination_path(self, blob: storage.Blob, destination_object: Optional[str], source_object_prefix_len: int)



   
   .. method:: _normalize_directory_path(self, source_object: Optional[str])



   
   .. staticmethod:: _prepare_sync_plan(source_bucket: storage.Bucket, destination_bucket: storage.Bucket, source_object: Optional[str], destination_object: Optional[str], recursive: bool)




.. function:: gcs_object_is_directory(bucket: str) -> bool
   Return True if given Google Cloud Storage URL (gs://<bucket>/<blob>)
   is a directory or an empty bucket. Otherwise return False.


.. function:: _parse_gcs_url(gsurl: str) -> Tuple[str, str]
   Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
   tuple containing the corresponding bucket and blob.


