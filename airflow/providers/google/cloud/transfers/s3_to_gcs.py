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

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url, gcs_object_is_directory

try:
    from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
except ImportError:
    from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator  # type: ignore[no-redef]

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToGCSOperator(S3ListOperator):
    """
    Synchronizes an S3 key, possibly a prefix, with a Google Cloud Storage
    destination path.

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
    :param gzip: Option to compress file for upload
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).


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

    def execute(self, context: Context):
        self._check_inputs()
        # use the super method to list all the files in an S3 bucket/key
        s3_objects = super().execute(context)

        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.google_impersonation_chain,
        )
        if not self.replace:
            s3_objects = self.exclude_existing_objects(s3_objects=s3_objects, gcs_hook=gcs_hook)

        if s3_objects:
            hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

            dest_gcs_bucket, dest_gcs_object_prefix = _parse_gcs_url(self.dest_gcs)
            for obj in s3_objects:
                # GCS hook builds its own in-memory file, so we have to create
                # and pass the path
                file_object = hook.get_key(obj, self.bucket)
                with NamedTemporaryFile(mode="wb", delete=True) as file:
                    file_object.download_fileobj(file)
                    file.flush()
                    gcs_file = self.s3_to_gcs_object(s3_object=obj)
                    gcs_hook.upload(dest_gcs_bucket, gcs_file, file.name, gzip=self.gzip)

            self.log.info("All done, uploaded %d files to Google Cloud Storage", len(s3_objects))
        else:
            self.log.info("In sync, no files needed to be uploaded to Google Cloud Storage")

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
        Transforms S3 path to GCS path according to the operator's logic.

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
        Transforms GCS path to S3 path according to the operator's logic.

        If apply_gcs_prefix == True then <gcs_prefix><content> => <s3_prefix><content>
        If apply_gcs_prefix == False then <gcs_prefix><s3_prefix><content> => <s3_prefix><content>

        """
        gcs_bucket, gcs_prefix = _parse_gcs_url(self.dest_gcs)
        s3_object = gcs_object.replace(gcs_prefix, "", 1)
        if self.apply_gcs_prefix:
            return self.prefix + s3_object
        return s3_object
