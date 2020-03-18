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
"""
This module contains a Google Cloud Storage Bucket operator.
"""
import subprocess
import sys
import warnings
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Union

from google.api_core.exceptions import Conflict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.xcom import MAX_XCOM_SIZE
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class GCSCreateBucketOperator(BaseOperator):
    """
    Creates a new bucket. Google Cloud Storage uses a flat namespace,
    so you can't create a bucket with a name that is already in use.

        .. seealso::
            For more information, see Bucket Naming Guidelines:
            https://cloud.google.com/storage/docs/bucketnaming.html#requirements

    :param bucket_name: The name of the bucket. (templated)
    :type bucket_name: str
    :param resource: An optional dict with parameters for creating the bucket.
            For information on available parameters, see Cloud Storage API doc:
            https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
    :type resource: dict
    :param storage_class: This defines how objects in the bucket are stored
            and determines the SLA and the cost of storage (templated). Values include

            - ``MULTI_REGIONAL``
            - ``REGIONAL``
            - ``STANDARD``
            - ``NEARLINE``
            - ``COLDLINE``.

            If this value is not specified when the bucket is
            created, it will default to STANDARD.
    :type storage_class: str
    :param location: The location of the bucket. (templated)
        Object data for objects in the bucket resides in physical storage
        within this region. Defaults to US.

        .. seealso:: https://developers.google.com/storage/docs/bucket-locations

    :type location: str
    :param project_id: The ID of the GCP Project. (templated)
    :type project_id: str
    :param labels: User-provided labels, in key/value pairs.
    :type labels: dict
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have domain-wide delegation enabled.
    :type delegate_to: str

    The following Operator would create a new bucket ``test-bucket``
    with ``MULTI_REGIONAL`` storage class in ``EU`` region

    .. code-block:: python

        CreateBucket = GoogleCloudStorageCreateBucketOperator(
            task_id='CreateNewBucket',
            bucket_name='test-bucket',
            storage_class='MULTI_REGIONAL',
            location='EU',
            labels={'env': 'dev', 'team': 'airflow'},
            gcp_conn_id='airflow-conn-id'
        )

    """
    template_fields = ('bucket_name', 'storage_class',
                       'location', 'project_id')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 resource: Optional[Dict] = None,
                 storage_class: str = 'MULTI_REGIONAL',
                 location: str = 'US',
                 project_id: Optional[str] = None,
                 labels: Optional[Dict] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket_name = bucket_name
        self.resource = resource
        self.storage_class = storage_class
        self.location = location
        self.project_id = project_id
        self.labels = labels
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
        try:
            hook.create_bucket(bucket_name=self.bucket_name,
                               resource=self.resource,
                               storage_class=self.storage_class,
                               location=self.location,
                               project_id=self.project_id,
                               labels=self.labels)
        except Conflict:  # HTTP 409
            self.log.warning("Bucket %s already exists", self.bucket_name)


class GCSListObjectsOperator(BaseOperator):
    """
    List all objects from the bucket with the give string prefix and delimiter in name.

    This operator returns a python list with the name of objects which can be used by
     `xcom` in the downstream task.

    :param bucket: The Google Cloud Storage bucket to find the objects. (templated)
    :type bucket: str
    :param prefix: Prefix string which filters objects whose name begin with
           this prefix. (templated)
    :type prefix: str
    :param delimiter: The delimiter by which you want to filter the objects. (templated)
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :type delimiter: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id:
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str

    **Example**:
        The following Operator would list all the Avro files from ``sales/sales-2017``
        folder in ``data`` bucket. ::

            GCS_Files = GoogleCloudStorageListOperator(
                task_id='GCS_Files',
                bucket='data',
                prefix='sales/sales-2017/',
                delimiter='.avro',
                gcp_conn_id=google_cloud_conn_id
            )
    """
    template_fields = ('bucket', 'prefix', 'delimiter')  # type: Iterable[str]

    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 prefix: Optional[str] = None,
                 delimiter: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):

        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

        self.log.info('Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s',
                      self.bucket, self.delimiter, self.prefix)

        return hook.list(bucket_name=self.bucket,
                         prefix=self.prefix,
                         delimiter=self.delimiter)


class GCSToLocalOperator(BaseOperator):
    """
    Downloads a file from Google Cloud Storage.

    If a filename is supplied, it writes the file to the specified location, alternatively one can
    set the ``store_to_xcom_key`` parameter to True push the file content into xcom. When the file size
    exceeds the maximum size for xcom it is recommended to write to a file.

    :param bucket: The Google Cloud Storage bucket where the object is.
        Must not contain 'gs://' prefix. (templated)
    :type bucket: str
    :param object: The name of the object to download in the Google cloud
        storage bucket. (templated)
    :type object: str
    :param filename: The file path, including filename,  on the local file system (where the
        operator is being executed) that the file should be downloaded to. (templated)
        If no filename passed, the downloaded data will not be stored on the local file
        system.
    :type filename: str
    :param store_to_xcom_key: If this param is set, the operator will push
        the contents of the downloaded file to XCom with the key set in this
        parameter. If not set, the downloaded data will not be pushed to XCom. (templated)
    :type store_to_xcom_key: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('bucket', 'object', 'filename', 'store_to_xcom_key',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 object_name: Optional[str] = None,
                 filename: Optional[str] = None,
                 store_to_xcom_key: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        # To preserve backward compatibility
        # TODO: Remove one day
        if object_name is None:
            if 'object' in kwargs:
                object_name = kwargs['object']
                DeprecationWarning("Use 'object_name' instead of 'object'.")
            else:
                TypeError("__init__() missing 1 required positional argument: 'object_name'")

        if filename is not None and store_to_xcom_key is not None:
            raise ValueError("Either filename or store_to_xcom_key can be set")

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object_name
        self.filename = filename
        self.store_to_xcom_key = store_to_xcom_key
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        self.log.info('Executing download: %s, %s, %s', self.bucket,
                      self.object, self.filename)
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

        if self.store_to_xcom_key:
            file_bytes = hook.download(bucket_name=self.bucket,
                                       object_name=self.object)
            if sys.getsizeof(file_bytes) < MAX_XCOM_SIZE:
                context['ti'].xcom_push(key=self.store_to_xcom_key, value=file_bytes)
            else:
                raise AirflowException(
                    'The size of the downloaded file is too large to push to XCom!'
                )
        else:
            hook.download(bucket_name=self.bucket,
                          object_name=self.object,
                          filename=self.filename)


class GCSDeleteObjectsOperator(BaseOperator):
    """
    Deletes objects from a Google Cloud Storage bucket, either
    from an explicit list of object names or all objects
    matching a prefix.

    :param bucket_name: The GCS bucket to delete from
    :type bucket_name: str
    :param objects: List of objects to delete. These should be the names
        of objects in the bucket, not including gs://bucket/
    :type objects: Iterable[str]
    :param prefix: Prefix of objects to delete. All objects matching this
        prefix in the bucket will be deleted.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('bucket_name', 'prefix', 'objects')

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 objects: Optional[Iterable[str]] = None,
                 prefix: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 *args, **kwargs) -> None:

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket_name = bucket_name
        self.objects = objects
        self.prefix = prefix
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

        if not objects and not prefix:
            raise ValueError("Either object or prefix should be set. Both are None")

        super().__init__(*args, **kwargs)

    def execute(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

        if self.objects:
            objects = self.objects
        else:
            objects = hook.list(bucket_name=self.bucket_name,
                                prefix=self.prefix)

        self.log.info("Deleting %s objects from %s",
                      len(objects), self.bucket_name)
        for object_name in objects:
            hook.delete(bucket_name=self.bucket_name,
                        object_name=object_name)


class GCSBucketCreateAclEntryOperator(BaseOperator):
    """
    Creates a new ACL entry on the specified bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSBucketCreateAclEntryOperator`

    :param bucket: Name of a bucket.
    :type bucket: str
    :param entity: The entity holding the permission, in one of the following forms:
        user-userId, user-email, group-groupId, group-email, domain-domain,
        project-team-projectId, allUsers, allAuthenticatedUsers
    :type entity: str
    :param role: The access permission for the entity.
        Acceptable values are: "OWNER", "READER", "WRITER".
    :type role: str
    :param user_project: (Optional) The project to be billed for this request.
        Required for Requester Pays buckets.
    :type user_project: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    """
    # [START gcs_bucket_create_acl_template_fields]
    template_fields = ('bucket', 'entity', 'role', 'user_project')
    # [END gcs_bucket_create_acl_template_fields]

    @apply_defaults
    def __init__(
        self,
        bucket: str,
        entity: str,
        role: str,
        user_project: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        google_cloud_storage_conn_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket = bucket
        self.entity = entity
        self.role = role
        self.user_project = user_project
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id
        )
        hook.insert_bucket_acl(bucket_name=self.bucket, entity=self.entity, role=self.role,
                               user_project=self.user_project)


class GCSObjectCreateAclEntryOperator(BaseOperator):
    """
    Creates a new ACL entry on the specified object.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSObjectCreateAclEntryOperator`

    :param bucket: Name of a bucket.
    :type bucket: str
    :param object_name: Name of the object. For information about how to URL encode object
        names to be path safe, see:
        https://cloud.google.com/storage/docs/json_api/#encoding
    :type object_name: str
    :param entity: The entity holding the permission, in one of the following forms:
        user-userId, user-email, group-groupId, group-email, domain-domain,
        project-team-projectId, allUsers, allAuthenticatedUsers
    :type entity: str
    :param role: The access permission for the entity.
        Acceptable values are: "OWNER", "READER".
    :type role: str
    :param generation: Optional. If present, selects a specific revision of this object.
    :type generation: long
    :param user_project: (Optional) The project to be billed for this request.
        Required for Requester Pays buckets.
    :type user_project: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    """
    # [START gcs_object_create_acl_template_fields]
    template_fields = ('bucket', 'object_name', 'entity', 'generation', 'role', 'user_project')
    # [END gcs_object_create_acl_template_fields]

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 object_name: str,
                 entity: str,
                 role: str,
                 generation: Optional[int] = None,
                 user_project: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket = bucket
        self.object_name = object_name
        self.entity = entity
        self.role = role
        self.generation = generation
        self.user_project = user_project
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id
        )
        hook.insert_object_acl(bucket_name=self.bucket,
                               object_name=self.object_name,
                               entity=self.entity,
                               role=self.role,
                               generation=self.generation,
                               user_project=self.user_project)


class GcsFileTransformOperator(BaseOperator):
    """
    Copies data from a source GCS location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation script and uploads the output to a destination bucket.
    If the output bucket is not specified the original file will be
    overwritten.

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file.

    :param source_bucket: The key to be retrieved from S3. (templated)
    :type source_bucket: str
    :param destination_bucket: The key to be written from S3. (templated)
    :type destination_bucket: str
    :param transform_script: location of the executable transformation script or list of arguments
        passed to subprocess ex. `['python', 'script.py', 10]`. (templated)
    :type transform_script: Union[str, List[str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ('source_bucket', 'destination_bucket', 'transform_script')

    @apply_defaults
    def __init__(
        self,
        source_bucket: str,
        source_object: str,
        transform_script: Union[str, List[str]],
        destination_bucket: Optional[str] = None,
        destination_object: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_bucket = destination_bucket or self.source_bucket
        self.destination_object = destination_object or self.source_object

        self.gcp_conn_id = gcp_conn_id
        self.transform_script = transform_script
        self.output_encoding = sys.getdefaultencoding()

    def execute(self, context: Dict):
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        with NamedTemporaryFile() as source_file, NamedTemporaryFile() as destination_file:
            self.log.info("Downloading file from %s", self.source_bucket)
            hook.download(
                bucket_name=self.source_bucket,
                object_name=self.source_object,
                filename=source_file.name
            )

            self.log.info("Starting the transformation")
            cmd = [self.transform_script] if isinstance(self.transform_script, str) else self.transform_script
            cmd += [source_file.name, destination_file.name]
            process = subprocess.Popen(
                args=cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                close_fds=True
            )
            self.log.info("Process output:")
            for line in iter(process.stdout.readline, b''):
                self.log.info(line.decode(self.output_encoding).rstrip())

            process.wait()
            if process.returncode > 0:
                raise AirflowException(
                    "Transform script failed: {0}".format(process.returncode)
                )

            self.log.info(
                "Transformation succeeded. Output temporarily located at %s",
                destination_file.name
            )

            self.log.info(
                "Uploading file to %s as %s",
                self.destination_bucket,
                self.destination_object
            )
            hook.upload(
                bucket_name=self.destination_bucket,
                object_name=self.destination_object,
                filename=destination_file.name
            )


class GCSDeleteBucketOperator(BaseOperator):
    """
    Deletes bucket from a Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSDeleteBucketOperator`

    :param bucket_name: name of the bucket which will be deleted
    :type bucket_name: str
    :param force: false not allow to delete non empty bucket, set force=True
        allows to delete non empty bucket
    :type: bool
    """

    template_fields = ('bucket_name', "gcp_conn_id")

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 force: bool = True,
                 gcp_conn_id: str = 'google_cloud_default',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.bucket_name = bucket_name
        self.force: bool = force
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_bucket(bucket_name=self.bucket_name, force=self.force)
