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
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    ACCESS_KEY_ID,
    AWS_ACCESS_KEY,
    AWS_S3_DATA_SOURCE,
    AWS_SECRET_ACCESS_KEY,
    BUCKET_NAME,
    GCS_DATA_SINK,
    INCLUDE_PREFIXES,
    OBJECT_CONDITIONS,
    OVERWRITE_OBJECTS_ALREADY_EXISTING_IN_SINK,
    PATH,
    PROJECT_ID,
    SCHEDULE,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    STATUS,
    TRANSFER_OPTIONS,
    TRANSFER_SPEC,
    CloudDataTransferServiceHook,
    GcpTransferJobsStatus,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url, gcs_object_is_directory
from airflow.providers.google.cloud.triggers.cloud_storage_transfer_service import (
    CloudStorageTransferServiceCreateJobsTrigger,
)

try:
    from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
except ImportError:
    from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator  # type: ignore[no-redef]

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class S3ToGCSOperator(S3ListOperator):
    """
    Synchronizes an S3 key, possibly a prefix, with a Google Cloud Storage destination path.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToGCSOperator`

    :param bucket: The S3 bucket where to find the objects. (templated)
    :param prefix: Prefix string which filters objects whose name begin with
        such prefix. (templated)
    :param apply_gcs_prefix: (Optional) Whether to replace source objects' path by given GCS destination path.
        If apply_gcs_prefix is False (default), then objects from S3 will be copied to GCS bucket into a given
        GSC path and the source path will be place inside. For example,
        <s3_bucket><s3_prefix><content> => <gcs_prefix><s3_prefix><content>

        If apply_gcs_prefix is True, then objects from S3 will be copied to GCS bucket into a given
        GCS path and the source path will be omitted. For example:
        <s3_bucket><s3_prefix><content> => <gcs_prefix><content>

    :param delimiter: the delimiter marks key hierarchy. (templated)
    :param aws_conn_id: The source S3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param dest_gcs: The destination Google Cloud Storage bucket and prefix
        where you want to store the files. (templated)
    :param replace: Whether you want to replace existing destination files
        or not.
    :param gzip: Option to compress file for upload. Parameter ignored in deferrable mode.
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode
    :param poll_interval: time in seconds between polling for job completion.
        The value is considered only when running in deferrable mode. Must be greater than 0.

    **Example**:

    .. code-block:: python

       s3_to_gcs_op = S3ToGCSOperator(
           task_id="s3_to_gcs_example",
           bucket="my-s3-bucket",
           prefix="data/customers-201804",
           gcp_conn_id="google_cloud_default",
           dest_gcs="gs://my.gcs.bucket/some/customers/",
           replace=False,
           gzip=True,
           dag=my_dag,
       )

    Note that ``bucket``, ``prefix``, ``delimiter`` and ``dest_gcs`` are
    templated, so you can use variables in them if you wish.
    """

    template_fields: Sequence[str] = (
        "bucket",
        "prefix",
        "delimiter",
        "dest_gcs",
        "google_impersonation_chain",
    )
    ui_color = "#e09411"
    transfer_job_max_files_number = 1000

    def __init__(
        self,
        *,
        bucket,
        prefix="",
        apply_gcs_prefix=False,
        delimiter="",
        aws_conn_id="aws_default",
        verify=None,
        gcp_conn_id="google_cloud_default",
        dest_gcs=None,
        replace=False,
        gzip=False,
        google_impersonation_chain: str | Sequence[str] | None = None,
        deferrable=conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        **kwargs,
    ):
        super().__init__(bucket=bucket, prefix=prefix, delimiter=delimiter, aws_conn_id=aws_conn_id, **kwargs)
        self.apply_gcs_prefix = apply_gcs_prefix
        self.gcp_conn_id = gcp_conn_id
        self.dest_gcs = dest_gcs
        self.replace = replace
        self.verify = verify
        self.gzip = gzip
        self.google_impersonation_chain = google_impersonation_chain
        self.deferrable = deferrable
        if poll_interval <= 0:
            raise ValueError("Invalid value for poll_interval. Expected value greater than 0")
        self.poll_interval = poll_interval

    def _check_inputs(self) -> None:
        if self.dest_gcs and not gcs_object_is_directory(self.dest_gcs):
            self.log.info(
                "Destination Google Cloud Storage path is not a valid "
                '"directory", define a path that ends with a slash "/" or '
                "leave it empty for the root of the bucket."
            )
            raise AirflowException(
                'The destination Google Cloud Storage path must end with a slash "/" or be empty.'
            )

    def _get_files(self, context: Context, gcs_hook: GCSHook) -> list[str]:
        # use the super method to list all the files in an S3 bucket/key
        s3_objects = super().execute(context)

        if not self.replace:
            s3_objects = self.exclude_existing_objects(s3_objects=s3_objects, gcs_hook=gcs_hook)

        return s3_objects

    def execute(self, context: Context):
        self._check_inputs()
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.google_impersonation_chain,
        )
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        s3_objects = self._get_files(context, gcs_hook)
        if not s3_objects:
            self.log.info("In sync, no files needed to be uploaded to Google Cloud Storage")

        elif self.deferrable:
            self.transfer_files_async(s3_objects, gcs_hook, s3_hook)
        else:
            self.transfer_files(s3_objects, gcs_hook, s3_hook)

        return s3_objects

    def exclude_existing_objects(self, s3_objects: list[str], gcs_hook: GCSHook) -> list[str]:
        """Excludes from the list objects that already exist in GCS bucket."""
        bucket_name, object_prefix = _parse_gcs_url(self.dest_gcs)

        existing_gcs_objects = set(gcs_hook.list(bucket_name, prefix=object_prefix))

        s3_paths = set(self.gcs_to_s3_object(gcs_object=gcs_object) for gcs_object in existing_gcs_objects)
        s3_objects_reduced = list(set(s3_objects) - s3_paths)

        if s3_objects_reduced:
            self.log.info("%s files are going to be synced: %s.", len(s3_objects_reduced), s3_objects_reduced)
        else:
            self.log.info("There are no new files to sync. Have a nice day!")
        return s3_objects_reduced

    def s3_to_gcs_object(self, s3_object: str) -> str:
        """
        Transform S3 path to GCS path according to the operator's logic.

        If apply_gcs_prefix == True then <s3_prefix><content> => <gcs_prefix><content>
        If apply_gcs_prefix == False then <s3_prefix><content> => <gcs_prefix><s3_prefix><content>

        """
        gcs_bucket, gcs_prefix = _parse_gcs_url(self.dest_gcs)
        if self.apply_gcs_prefix:
            gcs_object = s3_object.replace(self.prefix, gcs_prefix, 1)
            return gcs_object
        return gcs_prefix + s3_object

    def gcs_to_s3_object(self, gcs_object: str) -> str:
        """
        Transform GCS path to S3 path according to the operator's logic.

        If apply_gcs_prefix == True then <gcs_prefix><content> => <s3_prefix><content>
        If apply_gcs_prefix == False then <gcs_prefix><s3_prefix><content> => <s3_prefix><content>

        """
        gcs_bucket, gcs_prefix = _parse_gcs_url(self.dest_gcs)
        s3_object = gcs_object.replace(gcs_prefix, "", 1)
        if self.apply_gcs_prefix:
            return self.prefix + s3_object
        return s3_object

    def transfer_files(self, s3_objects: list[str], gcs_hook: GCSHook, s3_hook: S3Hook) -> None:
        if s3_objects:
            dest_gcs_bucket, dest_gcs_object_prefix = _parse_gcs_url(self.dest_gcs)
            for obj in s3_objects:
                # GCS hook builds its own in-memory file, so we have to create
                # and pass the path
                file_object = s3_hook.get_key(obj, self.bucket)
                with NamedTemporaryFile(mode="wb", delete=True) as file:
                    file_object.download_fileobj(file)
                    file.flush()
                    gcs_file = self.s3_to_gcs_object(s3_object=obj)
                    gcs_hook.upload(dest_gcs_bucket, gcs_file, file.name, gzip=self.gzip)

            self.log.info("All done, uploaded %d files to Google Cloud Storage", len(s3_objects))

    def transfer_files_async(self, files: list[str], gcs_hook: GCSHook, s3_hook: S3Hook) -> None:
        """Submit Google Cloud Storage Transfer Service job to copy files from AWS S3 to GCS."""
        if not len(files):
            raise ValueError("List of transferring files cannot be empty")
        job_names = self.submit_transfer_jobs(files=files, gcs_hook=gcs_hook, s3_hook=s3_hook)

        self.defer(
            trigger=CloudStorageTransferServiceCreateJobsTrigger(
                project_id=gcs_hook.project_id,
                gcp_conn_id=self.gcp_conn_id,
                job_names=job_names,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def submit_transfer_jobs(self, files: list[str], gcs_hook: GCSHook, s3_hook: S3Hook) -> list[str]:
        now = datetime.now(tz=timezone.utc)
        one_time_schedule = {"day": now.day, "month": now.month, "year": now.year}

        gcs_bucket, gcs_prefix = _parse_gcs_url(self.dest_gcs)
        config = s3_hook.conn_config

        body: dict[str, Any] = {
            PROJECT_ID: gcs_hook.project_id,
            STATUS: GcpTransferJobsStatus.ENABLED,
            SCHEDULE: {
                SCHEDULE_START_DATE: one_time_schedule,
                SCHEDULE_END_DATE: one_time_schedule,
            },
            TRANSFER_SPEC: {
                AWS_S3_DATA_SOURCE: {
                    BUCKET_NAME: self.bucket,
                    AWS_ACCESS_KEY: {
                        ACCESS_KEY_ID: config.aws_access_key_id,
                        AWS_SECRET_ACCESS_KEY: config.aws_secret_access_key,
                    },
                },
                OBJECT_CONDITIONS: {
                    INCLUDE_PREFIXES: [],
                },
                GCS_DATA_SINK: {BUCKET_NAME: gcs_bucket, PATH: gcs_prefix},
                TRANSFER_OPTIONS: {
                    OVERWRITE_OBJECTS_ALREADY_EXISTING_IN_SINK: self.replace,
                },
            },
        }

        # max size of the field 'transfer_job.transfer_spec.object_conditions.include_prefixes' is 1000,
        # that's why we submit multiple jobs transferring 1000 files each.
        # See documentation below
        # https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#ObjectConditions
        chunk_size = self.transfer_job_max_files_number
        job_names = []
        transfer_hook = self.get_transfer_hook()
        for i in range(0, len(files), chunk_size):
            files_chunk = files[i : i + chunk_size]
            body[TRANSFER_SPEC][OBJECT_CONDITIONS][INCLUDE_PREFIXES] = files_chunk
            job = transfer_hook.create_transfer_job(body=body)

            self.log.info("Submitted job %s to transfer %s file(s).", job["name"], len(files_chunk))
            job_names.append(job["name"])

        if len(files) > chunk_size:
            self.log.info("Overall submitted %s job(s) to transfer %s file(s).", len(job_names), len(files))

        return job_names

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Return immediately and relies on trigger to throw a success event. Callback for the trigger.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("%s completed with response %s ", self.task_id, event["message"])

    def get_transfer_hook(self):
        return CloudDataTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.google_impersonation_chain,
        )

    def get_openlineage_facets_on_start(self):
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url
        from airflow.providers.google.cloud.openlineage.utils import extract_ds_name_from_gcs_path
        from airflow.providers.openlineage.extractors import OperatorLineage

        gcs_bucket, gcs_blob = _parse_gcs_url(self.dest_gcs)
        if not self.apply_gcs_prefix:
            gcs_blob += self.prefix

        return OperatorLineage(
            inputs=[Dataset(namespace=f"s3://{self.bucket}", name=self.prefix.strip("/") or "/")],
            outputs=[Dataset(namespace=f"gs://{gcs_bucket}", name=extract_ds_name_from_gcs_path(gcs_blob))],
        )
