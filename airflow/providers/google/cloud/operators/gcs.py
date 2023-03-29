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
"""This module contains a Google Cloud Storage Bucket operator."""
from __future__ import annotations

import datetime
import subprocess
import sys
import warnings
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import TYPE_CHECKING, Sequence

import pendulum

if TYPE_CHECKING:
    from airflow.utils.context import Context

from google.api_core.exceptions import Conflict
from google.cloud.exceptions import GoogleCloudError

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.links.storage import FileDetailsLink, StorageLink
from airflow.utils import timezone


class GCSCreateBucketOperator(GoogleCloudBaseOperator):
    """
    Creates a new bucket. Google Cloud Storage uses a flat namespace,
    so you can't create a bucket with a name that is already in use.

        .. seealso::
            For more information, see Bucket Naming Guidelines:
            https://cloud.google.com/storage/docs/bucketnaming.html#requirements

    :param bucket_name: The name of the bucket. (templated)
    :param resource: An optional dict with parameters for creating the bucket.
            For information on available parameters, see Cloud Storage API doc:
            https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
    :param storage_class: This defines how objects in the bucket are stored
            and determines the SLA and the cost of storage (templated). Values include

            - ``MULTI_REGIONAL``
            - ``REGIONAL``
            - ``STANDARD``
            - ``NEARLINE``
            - ``COLDLINE``.

            If this value is not specified when the bucket is
            created, it will default to STANDARD.
    :param location: The location of the bucket. (templated)
        Object data for objects in the bucket resides in physical storage
        within this region. Defaults to US.

        .. seealso:: https://developers.google.com/storage/docs/bucket-locations

    :param project_id: The ID of the Google Cloud Project. (templated)
    :param labels: User-provided labels, in key/value pairs.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    The following Operator would create a new bucket ``test-bucket``
    with ``MULTI_REGIONAL`` storage class in ``EU`` region

    .. code-block:: python

        CreateBucket = GoogleCloudStorageCreateBucketOperator(
            task_id="CreateNewBucket",
            bucket_name="test-bucket",
            storage_class="MULTI_REGIONAL",
            location="EU",
            labels={"env": "dev", "team": "airflow"},
            gcp_conn_id="airflow-conn-id",
        )

    """

    template_fields: Sequence[str] = (
        "bucket_name",
        "storage_class",
        "location",
        "project_id",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"
    operator_extra_links = (StorageLink(),)

    def __init__(
        self,
        *,
        bucket_name: str,
        resource: dict | None = None,
        storage_class: str = "MULTI_REGIONAL",
        location: str = "US",
        project_id: str | None = None,
        labels: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.resource = resource
        self.storage_class = storage_class
        self.location = location
        self.project_id = project_id
        self.labels = labels
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        StorageLink.persist(
            context=context,
            task_instance=self,
            uri=self.bucket_name,
            project_id=self.project_id or hook.project_id,
        )
        try:
            hook.create_bucket(
                bucket_name=self.bucket_name,
                resource=self.resource,
                storage_class=self.storage_class,
                location=self.location,
                project_id=self.project_id,
                labels=self.labels,
            )
        except Conflict:  # HTTP 409
            self.log.warning("Bucket %s already exists", self.bucket_name)


class GCSListObjectsOperator(GoogleCloudBaseOperator):
    """
    List all objects from the bucket with the given string prefix and delimiter in name.

    This operator returns a python list with the name of objects which can be used by
    XCom in the downstream task.

    :param bucket: The Google Cloud Storage bucket to find the objects. (templated)
    :param prefix: Prefix string which filters objects whose name begin with
           this prefix. (templated)
    :param delimiter: The delimiter by which you want to filter the objects. (templated)
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

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

    template_fields: Sequence[str] = (
        "bucket",
        "prefix",
        "delimiter",
        "impersonation_chain",
    )

    ui_color = "#f0eee4"

    operator_extra_links = (StorageLink(),)

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str | None = None,
        delimiter: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list:

        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info(
            "Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s",
            self.bucket,
            self.delimiter,
            self.prefix,
        )

        StorageLink.persist(
            context=context,
            task_instance=self,
            uri=self.bucket,
            project_id=hook.project_id,
        )

        return hook.list(bucket_name=self.bucket, prefix=self.prefix, delimiter=self.delimiter)


class GCSDeleteObjectsOperator(GoogleCloudBaseOperator):
    """
    Deletes objects from a Google Cloud Storage bucket, either
    from an explicit list of object names or all objects
    matching a prefix.

    :param bucket_name: The GCS bucket to delete from
    :param objects: List of objects to delete. These should be the names
        of objects in the bucket, not including gs://bucket/
    :param prefix: Prefix of objects to delete. All objects matching this
        prefix in the bucket will be deleted.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "bucket_name",
        "prefix",
        "objects",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        bucket_name: str,
        objects: list[str] | None = None,
        prefix: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:

        self.bucket_name = bucket_name
        self.objects = objects
        self.prefix = prefix
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

        if objects is None and prefix is None:
            err_message = "(Task {task_id}) Either object or prefix should be set. Both are None.".format(
                **kwargs
            )
            raise ValueError(err_message)

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        if self.objects:
            objects = self.objects
        else:
            objects = hook.list(bucket_name=self.bucket_name, prefix=self.prefix)

        self.log.info("Deleting %s objects from %s", len(objects), self.bucket_name)
        for object_name in objects:
            hook.delete(bucket_name=self.bucket_name, object_name=object_name)


class GCSBucketCreateAclEntryOperator(GoogleCloudBaseOperator):
    """
    Creates a new ACL entry on the specified bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSBucketCreateAclEntryOperator`

    :param bucket: Name of a bucket.
    :param entity: The entity holding the permission, in one of the following forms:
        user-userId, user-email, group-groupId, group-email, domain-domain,
        project-team-projectId, allUsers, allAuthenticatedUsers
    :param role: The access permission for the entity.
        Acceptable values are: "OWNER", "READER", "WRITER".
    :param user_project: (Optional) The project to be billed for this request.
        Required for Requester Pays buckets.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcs_bucket_create_acl_template_fields]
    template_fields: Sequence[str] = (
        "bucket",
        "entity",
        "role",
        "user_project",
        "impersonation_chain",
    )
    # [END gcs_bucket_create_acl_template_fields]
    operator_extra_links = (StorageLink(),)

    def __init__(
        self,
        *,
        bucket: str,
        entity: str,
        role: str,
        user_project: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.entity = entity
        self.role = role
        self.user_project = user_project
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        StorageLink.persist(
            context=context,
            task_instance=self,
            uri=self.bucket,
            project_id=hook.project_id,
        )
        hook.insert_bucket_acl(
            bucket_name=self.bucket, entity=self.entity, role=self.role, user_project=self.user_project
        )


class GCSObjectCreateAclEntryOperator(GoogleCloudBaseOperator):
    """
    Creates a new ACL entry on the specified object.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSObjectCreateAclEntryOperator`

    :param bucket: Name of a bucket.
    :param object_name: Name of the object. For information about how to URL encode object
        names to be path safe, see:
        https://cloud.google.com/storage/docs/json_api/#encoding
    :param entity: The entity holding the permission, in one of the following forms:
        user-userId, user-email, group-groupId, group-email, domain-domain,
        project-team-projectId, allUsers, allAuthenticatedUsers
    :param role: The access permission for the entity.
        Acceptable values are: "OWNER", "READER".
    :param generation: Optional. If present, selects a specific revision of this object.
    :param user_project: (Optional) The project to be billed for this request.
        Required for Requester Pays buckets.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcs_object_create_acl_template_fields]
    template_fields: Sequence[str] = (
        "bucket",
        "object_name",
        "entity",
        "generation",
        "role",
        "user_project",
        "impersonation_chain",
    )
    # [END gcs_object_create_acl_template_fields]
    operator_extra_links = (FileDetailsLink(),)

    def __init__(
        self,
        *,
        bucket: str,
        object_name: str,
        entity: str,
        role: str,
        generation: int | None = None,
        user_project: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.object_name = object_name
        self.entity = entity
        self.role = role
        self.generation = generation
        self.user_project = user_project
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        FileDetailsLink.persist(
            context=context,
            task_instance=self,
            uri=f"{self.bucket}/{self.object_name}",
            project_id=hook.project_id,
        )
        hook.insert_object_acl(
            bucket_name=self.bucket,
            object_name=self.object_name,
            entity=self.entity,
            role=self.role,
            generation=self.generation,
            user_project=self.user_project,
        )


class GCSFileTransformOperator(GoogleCloudBaseOperator):
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

    :param source_bucket: The bucket to locate the source_object. (templated)
    :param source_object: The key to be retrieved from GCS. (templated)
    :param destination_bucket: The bucket to upload the key after transformation.
        If not provided, source_bucket will be used. (templated)
    :param destination_object: The key to be written in GCS.
        If not provided, source_object will be used. (templated)
    :param transform_script: location of the executable transformation script or list of arguments
        passed to subprocess ex. `['python', 'script.py', 10]`. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "source_bucket",
        "source_object",
        "destination_bucket",
        "destination_object",
        "transform_script",
        "impersonation_chain",
    )
    operator_extra_links = (FileDetailsLink(),)

    def __init__(
        self,
        *,
        source_bucket: str,
        source_object: str,
        transform_script: str | list[str],
        destination_bucket: str | None = None,
        destination_object: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_bucket = destination_bucket or self.source_bucket
        self.destination_object = destination_object or self.source_object

        self.gcp_conn_id = gcp_conn_id
        self.transform_script = transform_script
        self.output_encoding = sys.getdefaultencoding()
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        with NamedTemporaryFile() as source_file, NamedTemporaryFile() as destination_file:
            self.log.info("Downloading file from %s", self.source_bucket)
            hook.download(
                bucket_name=self.source_bucket, object_name=self.source_object, filename=source_file.name
            )

            self.log.info("Starting the transformation")
            cmd = [self.transform_script] if isinstance(self.transform_script, str) else self.transform_script
            cmd += [source_file.name, destination_file.name]
            with subprocess.Popen(
                args=cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True
            ) as process:
                self.log.info("Process output:")
                if process.stdout:
                    for line in iter(process.stdout.readline, b""):
                        self.log.info(line.decode(self.output_encoding).rstrip())

                process.wait()
                if process.returncode:
                    raise AirflowException(f"Transform script failed: {process.returncode}")

            self.log.info("Transformation succeeded. Output temporarily located at %s", destination_file.name)

            self.log.info("Uploading file to %s as %s", self.destination_bucket, self.destination_object)
            FileDetailsLink.persist(
                context=context,
                task_instance=self,
                uri=f"{self.destination_bucket}/{self.destination_object}",
                project_id=hook.project_id,
            )
            hook.upload(
                bucket_name=self.destination_bucket,
                object_name=self.destination_object,
                filename=destination_file.name,
            )


class GCSTimeSpanFileTransformOperator(GoogleCloudBaseOperator):
    """
    Determines a list of objects that were added or modified at a GCS source
    location during a specific time-span, copies them to a temporary location
    on the local file system, runs a transform on this file as specified by
    the transformation script and uploads the output to the destination bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSTimeSpanFileTransformOperator`

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
    transformation script. The time-span is passed to the transform script as
    third and fourth argument as UTC ISO 8601 string.

    The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file.

    :param source_bucket: The bucket to fetch data from. (templated)
    :param source_prefix: Prefix string which filters objects whose name begin with
           this prefix. Can interpolate execution date and time components. (templated)
    :param source_gcp_conn_id: The connection ID to use connecting to Google Cloud
           to download files to be processed.
    :param source_impersonation_chain: Optional service account to impersonate using short-term
        credentials (to download files to be processed), or chained list of accounts required to
        get the access_token of the last account in the list, which will be impersonated in the
        request. If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :param destination_bucket: The bucket to write data to. (templated)
    :param destination_prefix: Prefix string for the upload location.
        Can interpolate execution date and time components. (templated)
    :param destination_gcp_conn_id: The connection ID to use connecting to Google Cloud
           to upload processed files.
    :param destination_impersonation_chain: Optional service account to impersonate using short-term
        credentials (to upload processed files), or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :param transform_script: location of the executable transformation script or list of arguments
        passed to subprocess ex. `['python', 'script.py', 10]`. (templated)


    :param chunk_size: The size of a chunk of data when downloading or uploading (in bytes).
        This must be a multiple of 256 KB (per the google clout storage API specification).
    :param download_continue_on_fail: With this set to true, if a download fails the task does not error out
        but will still continue.
    :param upload_chunk_size: The size of a chunk of data when uploading (in bytes).
        This must be a multiple of 256 KB (per the google clout storage API specification).
    :param upload_continue_on_fail: With this set to true, if an upload fails the task does not error out
        but will still continue.
    :param upload_num_attempts: Number of attempts to try to upload a single file.
    """

    template_fields: Sequence[str] = (
        "source_bucket",
        "source_prefix",
        "destination_bucket",
        "destination_prefix",
        "transform_script",
        "source_impersonation_chain",
        "destination_impersonation_chain",
    )
    operator_extra_links = (StorageLink(),)

    @staticmethod
    def interpolate_prefix(prefix: str, dt: datetime.datetime) -> str | None:
        """Interpolate prefix with datetime.

        :param prefix: The prefix to interpolate
        :param dt: The datetime to interpolate

        """
        return dt.strftime(prefix) if prefix else None

    def __init__(
        self,
        *,
        source_bucket: str,
        source_prefix: str,
        source_gcp_conn_id: str,
        destination_bucket: str,
        destination_prefix: str,
        destination_gcp_conn_id: str,
        transform_script: str | list[str],
        source_impersonation_chain: str | Sequence[str] | None = None,
        destination_impersonation_chain: str | Sequence[str] | None = None,
        chunk_size: int | None = None,
        download_continue_on_fail: bool | None = False,
        download_num_attempts: int = 1,
        upload_continue_on_fail: bool | None = False,
        upload_num_attempts: int = 1,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix
        self.source_gcp_conn_id = source_gcp_conn_id
        self.source_impersonation_chain = source_impersonation_chain

        self.destination_bucket = destination_bucket
        self.destination_prefix = destination_prefix
        self.destination_gcp_conn_id = destination_gcp_conn_id
        self.destination_impersonation_chain = destination_impersonation_chain

        self.transform_script = transform_script
        self.output_encoding = sys.getdefaultencoding()

        self.chunk_size = chunk_size
        self.download_continue_on_fail = download_continue_on_fail
        self.download_num_attempts = download_num_attempts
        self.upload_continue_on_fail = upload_continue_on_fail
        self.upload_num_attempts = upload_num_attempts

    def execute(self, context: Context) -> list[str]:
        # Define intervals and prefixes.
        try:
            orig_start = context["data_interval_start"]
            orig_end = context["data_interval_end"]
        except KeyError:
            orig_start = pendulum.instance(context["execution_date"])
            following_execution_date = context["dag"].following_schedule(context["execution_date"])
            if following_execution_date is None:
                orig_end = None
            else:
                orig_end = pendulum.instance(following_execution_date)

        timespan_start = orig_start
        if orig_end is None:  # Only possible in Airflow before 2.2.
            self.log.warning("No following schedule found, setting timespan end to max %s", orig_end)
            timespan_end = pendulum.instance(datetime.datetime.max)
        elif orig_start >= orig_end:  # Airflow 2.2 sets start == end for non-perodic schedules.
            self.log.warning("DAG schedule not periodic, setting timespan end to max %s", orig_end)
            timespan_end = pendulum.instance(datetime.datetime.max)
        else:
            timespan_end = orig_end

        timespan_start = timespan_start.in_timezone(timezone.utc)
        timespan_end = timespan_end.in_timezone(timezone.utc)

        source_prefix_interp = GCSTimeSpanFileTransformOperator.interpolate_prefix(
            self.source_prefix,
            timespan_start,
        )
        destination_prefix_interp = GCSTimeSpanFileTransformOperator.interpolate_prefix(
            self.destination_prefix,
            timespan_start,
        )

        source_hook = GCSHook(
            gcp_conn_id=self.source_gcp_conn_id,
            impersonation_chain=self.source_impersonation_chain,
        )
        destination_hook = GCSHook(
            gcp_conn_id=self.destination_gcp_conn_id,
            impersonation_chain=self.destination_impersonation_chain,
        )
        StorageLink.persist(
            context=context,
            task_instance=self,
            uri=self.destination_bucket,
            project_id=destination_hook.project_id,
        )

        # Fetch list of files.
        blobs_to_transform = source_hook.list_by_timespan(
            bucket_name=self.source_bucket,
            prefix=source_prefix_interp,
            timespan_start=timespan_start,
            timespan_end=timespan_end,
        )

        with TemporaryDirectory() as temp_input_dir, TemporaryDirectory() as temp_output_dir:
            temp_input_dir_path = Path(temp_input_dir)
            temp_output_dir_path = Path(temp_output_dir)

            # TODO: download in parallel.
            for blob_to_transform in blobs_to_transform:
                destination_file = temp_input_dir_path / blob_to_transform
                destination_file.parent.mkdir(parents=True, exist_ok=True)
                try:
                    source_hook.download(
                        bucket_name=self.source_bucket,
                        object_name=blob_to_transform,
                        filename=str(destination_file),
                        chunk_size=self.chunk_size,
                        num_max_attempts=self.download_num_attempts,
                    )
                except GoogleCloudError:
                    if self.download_continue_on_fail:
                        continue
                    raise

            self.log.info("Starting the transformation")
            cmd = [self.transform_script] if isinstance(self.transform_script, str) else self.transform_script
            cmd += [
                str(temp_input_dir_path),
                str(temp_output_dir_path),
                timespan_start.replace(microsecond=0).isoformat(),
                timespan_end.replace(microsecond=0).isoformat(),
            ]
            with subprocess.Popen(
                args=cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True
            ) as process:
                self.log.info("Process output:")
                if process.stdout:
                    for line in iter(process.stdout.readline, b""):
                        self.log.info(line.decode(self.output_encoding).rstrip())

                process.wait()
                if process.returncode:
                    raise AirflowException(f"Transform script failed: {process.returncode}")

            self.log.info("Transformation succeeded. Output temporarily located at %s", temp_output_dir_path)

            files_uploaded = []

            # TODO: upload in parallel.
            for upload_file in temp_output_dir_path.glob("**/*"):
                if upload_file.is_dir():
                    continue

                upload_file_name = str(upload_file.relative_to(temp_output_dir_path))

                if self.destination_prefix is not None:
                    upload_file_name = f"{destination_prefix_interp}/{upload_file_name}"

                self.log.info("Uploading file %s to %s", upload_file, upload_file_name)

                try:
                    destination_hook.upload(
                        bucket_name=self.destination_bucket,
                        object_name=upload_file_name,
                        filename=str(upload_file),
                        chunk_size=self.chunk_size,
                        num_max_attempts=self.upload_num_attempts,
                    )
                    files_uploaded.append(str(upload_file_name))
                except GoogleCloudError:
                    if self.upload_continue_on_fail:
                        continue
                    raise

            return files_uploaded


class GCSDeleteBucketOperator(GoogleCloudBaseOperator):
    """
    Deletes bucket from a Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSDeleteBucketOperator`

    :param bucket_name: name of the bucket which will be deleted
    :param force: false not allow to delete non empty bucket, set force=True
        allows to delete non empty bucket
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "bucket_name",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        bucket_name: str,
        force: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.force: bool = force
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        hook.delete_bucket(bucket_name=self.bucket_name, force=self.force)


class GCSSynchronizeBucketsOperator(GoogleCloudBaseOperator):
    """
    Synchronizes the contents of the buckets or bucket's directories in the Google Cloud Services.

    Parameters ``source_object`` and ``destination_object`` describe the root sync directory. If they are
    not passed, the entire bucket will be synchronized. They should point to directories.

    .. note::
        The synchronization of individual files is not supported. Only entire directories can be
        synchronized.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSSynchronizeBuckets`

    :param source_bucket: The name of the bucket containing the source objects.
    :param destination_bucket: The name of the bucket containing the destination objects.
    :param source_object: The root sync directory in the source bucket.
    :param destination_object: The root sync directory in the destination bucket.
    :param recursive: If True, subdirectories will be considered
    :param allow_overwrite: if True, the files will be overwritten if a mismatched file is found.
        By default, overwriting files is not allowed
    :param delete_extra_files: if True, deletes additional files from the source that not found in the
        destination. By default extra files are not deleted.

        .. note::
            This option can delete data quickly if you specify the wrong source/destination combination.

    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "source_bucket",
        "destination_bucket",
        "source_object",
        "destination_object",
        "recursive",
        "delete_extra_files",
        "allow_overwrite",
        "gcp_conn_id",
        "delegate_to",
        "impersonation_chain",
    )
    operator_extra_links = (StorageLink(),)

    def __init__(
        self,
        *,
        source_bucket: str,
        destination_bucket: str,
        source_object: str | None = None,
        destination_object: str | None = None,
        recursive: bool = True,
        delete_extra_files: bool = False,
        allow_overwrite: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.source_object = source_object
        self.destination_object = destination_object
        self.recursive = recursive
        self.delete_extra_files = delete_extra_files
        self.allow_overwrite = allow_overwrite
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        StorageLink.persist(
            context=context,
            task_instance=self,
            uri=self._get_uri(self.destination_bucket, self.destination_object),
            project_id=hook.project_id,
        )
        hook.sync(
            source_bucket=self.source_bucket,
            destination_bucket=self.destination_bucket,
            source_object=self.source_object,
            destination_object=self.destination_object,
            recursive=self.recursive,
            delete_extra_files=self.delete_extra_files,
            allow_overwrite=self.allow_overwrite,
        )

    def _get_uri(self, gcs_bucket: str, gcs_object: str | None) -> str:
        if gcs_object and gcs_object[-1] == "/":
            gcs_object = gcs_object[:-1]
        return f"{gcs_bucket}/{gcs_object}" if gcs_object else gcs_bucket
