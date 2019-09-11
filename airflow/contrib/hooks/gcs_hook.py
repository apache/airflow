# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import gzip as gz
import os
import shutil
import warnings

from google.cloud import storage

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.exceptions import AirflowException


class GoogleCloudStorageHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Storage. This hook uses the Google Cloud Platform
    connection.
    """

    _conn = None

    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GoogleCloudStorageHook, self).__init__(google_cloud_storage_conn_id,
                                                     delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        if not self._conn:
            self._conn = storage.Client(credentials=self._get_credentials(),
                                        project=self.project_id)

        return self._conn

    # pylint:disable=redefined-builtin
    def copy(self, source_bucket, source_object, destination_bucket=None,
             destination_object=None):
        """
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
        """
        destination_bucket = destination_bucket or source_bucket
        destination_object = destination_object or source_object
        if source_bucket == destination_bucket and \
                source_object == destination_object:

            raise ValueError(
                'Either source/destination bucket or source/destination object '
                'must be different, not both the same: bucket=%s, object=%s' %
                (source_bucket, source_object))
        if not source_bucket or not source_object:
            raise ValueError('source_bucket and source_object cannot be empty.')

        client = self.get_conn()
        source_bucket = client.bucket(source_bucket)
        source_object = source_bucket.blob(source_object)
        destination_bucket = client.bucket(destination_bucket)
        destination_object = source_bucket.copy_blob(
            blob=source_object,
            destination_bucket=destination_bucket,
            new_name=destination_object)

        self.log.info('Object %s in bucket %s copied to object %s in bucket %s',
                      source_object.name, source_bucket.name,
                      destination_object.name, destination_bucket.name)

    def rewrite(self, source_bucket, source_object, destination_bucket,
                destination_object=None):
        """
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
        """
        destination_object = destination_object or source_object
        if (source_bucket == destination_bucket and
                source_object == destination_object):
            raise ValueError(
                'Either source/destination bucket or source/destination object '
                'must be different, not both the same: bucket=%s, object=%s' %
                (source_bucket, source_object))
        if not source_bucket or not source_object:
            raise ValueError('source_bucket and source_object cannot be empty.')

        client = self.get_conn()
        source_bucket = client.bucket(source_bucket)
        source_object = source_bucket.blob(blob_name=source_object)
        destination_bucket = client.bucket(destination_bucket)

        token, bytes_rewritten, total_bytes = destination_bucket.blob(
            blob_name=destination_object).rewrite(
            source=source_object
        )

        self.log.info('Total Bytes: %s | Bytes Written: %s',
                      total_bytes, bytes_rewritten)

        while token is not None:
            token, bytes_rewritten, total_bytes = destination_bucket.blob(
                blob_name=destination_object).rewrite(
                source=source_object, token=token
            )

            self.log.info('Total Bytes: %s | Bytes Written: %s',
                          total_bytes, bytes_rewritten)
        self.log.info('Object %s in bucket %s copied to object %s in bucket %s',
                      source_object.name, source_bucket.name,
                      destination_object, destination_bucket.name)

    # pylint:disable=redefined-builtin
    def download(self, bucket, object, filename=None):
        """
        Get a file from Google Cloud Storage.

        :param bucket: The bucket to fetch from.
        :type bucket: str
        :param object: The object to fetch.
        :type object: str
        :param filename: If set, a local file path where the file should be written to.
        :type filename: str
        """
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.blob(blob_name=object)

        if filename:
            blob.download_to_filename(filename)
            self.log.info('File downloaded to %s', filename)

        return blob.download_as_string()

    # pylint:disable=redefined-builtin
    def upload(self, bucket, object, filename,
               mime_type='application/octet-stream', gzip=False,
               multipart=None, num_retries=None):
        """
        Uploads a local file to Google Cloud Storage.

        :param bucket: The bucket to upload to.
        :type bucket: str
        :param object: The object name to set when uploading the local file.
        :type object: str
        :param filename: The local file path to the file to be uploaded.
        :type filename: str
        :param mime_type: The MIME type to set when uploading the file.
        :type mime_type: str
        :param gzip: Option to compress file for upload
        :type gzip: bool
        """

        if multipart is not None:
            warnings.warn("'multipart' parameter is deprecated."
                          " It is handled automatically by the Storage client", DeprecationWarning)

        if num_retries is not None:
            warnings.warn("'num_retries' parameter is deprecated."
                          " It is handled automatically by the Storage client", DeprecationWarning)

        if gzip:
            filename_gz = filename + '.gz'

            with open(filename, 'rb') as f_in:
                with gz.open(filename_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz

        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.blob(blob_name=object)
        blob.upload_from_filename(filename=filename,
                                  content_type=mime_type)

        if gzip:
            os.remove(filename)
        self.log.info('File %s uploaded to %s in %s bucket', filename, object, bucket)

    # pylint:disable=redefined-builtin
    def exists(self, bucket, object):
        """
        Checks for the existence of a file in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        """
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.blob(blob_name=object)
        return blob.exists()

    # pylint:disable=redefined-builtin
    def is_updated_after(self, bucket, object, ts):
        """
        Checks if an object is updated in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        :param ts: The timestamp to check against.
        :type ts: datetime.datetime
        """
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.get_blob(blob_name=object)

        if blob is None:
            raise ValueError("Object ({}) not found in Bucket ({})".format(
                object, bucket))

        blob_update_time = blob.updated

        if blob_update_time is not None:
            import dateutil.tz

            if not ts.tzinfo:
                ts = ts.replace(tzinfo=dateutil.tz.tzutc())

            self.log.info("Verify object date: %s > %s", blob_update_time, ts)

            if blob_update_time > ts:
                return True

        return False

    def delete(self, bucket, object, generation=None):
        """
        Deletes an object from the bucket.

        :param bucket: name of the bucket, where the object resides
        :type bucket: str
        :param object: name of the object to delete
        :type object: str
        """

        if generation is not None:
            warnings.warn("'generation' parameter is no longer supported", DeprecationWarning)

        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.blob(blob_name=object)
        blob.delete()

        self.log.info('Blob %s deleted.', object)

    def list(self, bucket, versions=None, maxResults=None, prefix=None, delimiter=None):
        """
        List all objects from the bucket with the give string prefix in name

        :param bucket: bucket name
        :type bucket: str
        :param versions: if true, list all versions of the objects
        :type versions: bool
        :param maxResults: max count of items to return in a single page of responses
        :type maxResults: int
        :param prefix: prefix string which filters objects whose name begin with
            this prefix
        :type prefix: str
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        :type delimiter: str
        :return: a stream of object names matching the filtering criteria
        """
        client = self.get_conn()
        bucket = client.bucket(bucket)

        ids = []
        pageToken = None
        while True:
            blobs = bucket.list_blobs(
                max_results=maxResults,
                page_token=pageToken,
                prefix=prefix,
                delimiter=delimiter,
                versions=versions
            )

            blob_names = []
            for blob in blobs:
                blob_names.append(blob.name)

            prefixes = blobs.prefixes
            if prefixes:
                ids += list(prefixes)
            else:
                ids += blob_names

            pageToken = blobs.next_page_token
            if pageToken is None:
                # empty next page token
                break
        return ids

    def get_size(self, bucket, object):
        """
        Gets the size of a file in Google Cloud Storage in bytes.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud storage bucket.
        :type object: str

        """
        self.log.info('Checking the file size of object: %s in bucket: %s',
                      object,
                      bucket)
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.get_blob(blob_name=object)
        blob_size = blob.size
        self.log.info('The file size of %s is %s bytes.', object, blob_size)
        return blob_size

    def get_crc32c(self, bucket, object):
        """
        Gets the CRC32c checksum of an object in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        """
        self.log.info('Retrieving the crc32c checksum of '
                      'object: %s in bucket: %s', object, bucket)
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.get_blob(blob_name=object)
        blob_crc32c = blob.crc32c
        self.log.info('The crc32c checksum of %s is %s', object, blob_crc32c)
        return blob_crc32c

    def get_md5hash(self, bucket, object):
        """
        Gets the MD5 hash of an object in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        """
        self.log.info('Retrieving the MD5 hash of '
                      'object: %s in bucket: %s', object, bucket)
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.get_blob(blob_name=object)
        blob_md5hash = blob.md5_hash
        self.log.info('The md5Hash of %s is %s', object, blob_md5hash)
        return blob_md5hash

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_bucket(self,
                      bucket_name,
                      resource=None,
                      storage_class='MULTI_REGIONAL',
                      location='US',
                      project_id=None,
                      labels=None
                      ):
        """
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
        :param project_id: The ID of the GCP Project.
        :type project_id: str
        :param labels: User-provided labels, in key/value pairs.
        :type labels: dict
        :return: If successful, it returns the ``id`` of the bucket.
        """

        self.log.info('Creating Bucket: %s; Location: %s; Storage Class: %s',
                      bucket_name, location, storage_class)

        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket_name)
        bucket_resource = resource or {}

        for item in bucket_resource:
            if item != "name":
                bucket._patch_property(name=item, value=resource[item])

        bucket.storage_class = storage_class
        bucket.labels = labels or {}
        bucket.create(project=project_id, location=location)
        return bucket.id

    def insert_bucket_acl(self, bucket, entity, role, user_project=None):
        """
        Creates a new ACL entry on the specified bucket.
        See: https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls/insert

        :param bucket: Name of a bucket.
        :type bucket: str
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
        """
        self.log.info('Creating a new ACL entry in bucket: %s', bucket)
        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket)
        bucket.acl.reload()
        bucket.acl.entity_from_dict(entity_dict={"entity": entity, "role": role})
        if user_project:
            bucket.acl.user_project = user_project
        bucket.acl.save()

        self.log.info('A new ACL entry created in bucket: %s', bucket)

    def insert_object_acl(self, bucket, object_name, entity, role, generation=None,
                          user_project=None):
        """
        Creates a new ACL entry on the specified object.
        See: https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert

        :param bucket: Name of a bucket.
        :type bucket: str
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
        :param user_project: (Optional) The project to be billed for this request.
            Required for Requester Pays buckets.
        :type user_project: str
        """
        if generation is not None:
            warnings.warn("'generation' parameter is no longer supported", DeprecationWarning)
        self.log.info('Creating a new ACL entry for object: %s in bucket: %s',
                      object_name, bucket)
        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket)
        blob = bucket.blob(object_name)
        # Reload fetches the current ACL from Cloud Storage.
        blob.acl.reload()
        blob.acl.entity_from_dict(entity_dict={"entity": entity, "role": role})
        if user_project:
            blob.acl.user_project = user_project
        blob.acl.save()

        self.log.info('A new ACL entry created for object: %s in bucket: %s',
                      object_name, bucket)

    def compose(self, bucket, source_objects, destination_object, num_retries=None):
        """
        Composes a list of existing object into a new object in the same storage bucket

        Currently it only supports up to 32 objects that can be concatenated
        in a single operation

        https://cloud.google.com/storage/docs/json_api/v1/objects/compose

        :param bucket: The name of the bucket containing the source objects.
            This is also the same bucket to store the composed destination object.
        :type bucket: str
        :param source_objects: The list of source objects that will be composed
            into a single object.
        :type source_objects: list
        :param destination_object: The path of the object if given.
        :type destination_object: str
        """
        if num_retries is not None:
            warnings.warn("'num_retries' parameter is Deprecated. Retries are "
                          "now handled automatically", DeprecationWarning)

        if not source_objects or not len(source_objects):
            raise ValueError('source_objects cannot be empty.')

        if not bucket or not destination_object:
            raise ValueError('bucket and destination_object cannot be empty.')

        self.log.info("Composing %s to %s in the bucket %s",
                      source_objects, destination_object, bucket)
        client = self.get_conn()
        bucket = client.bucket(bucket)
        destination_blob = bucket.blob(destination_object)
        destination_blob.compose(
            sources=[
                bucket.blob(blob_name=source_object) for source_object in source_objects
            ])

        self.log.info("Completed successfully.")


def _parse_gcs_url(gsurl):
    """
    Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
    tuple containing the corresponding bucket and blob.
    """
    # Python 3
    try:
        from urllib.parse import urlparse
    # Python 2
    except ImportError:
        from urlparse import urlparse

    parsed_url = urlparse(gsurl)
    if not parsed_url.netloc:
        raise AirflowException('Please provide a bucket name')
    else:
        bucket = parsed_url.netloc
        # Remove leading '/' but NOT trailing one
        blob = parsed_url.path.lstrip('/')
        return bucket, blob
