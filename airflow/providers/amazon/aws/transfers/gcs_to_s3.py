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
"""This module contains Google Cloud Storage to S3 operator."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Sequence

from packaging.version import Version

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GCSToS3Operator(BaseOperator):
    """
    Synchronizes a Google Cloud Storage bucket with an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToS3Operator`

    :param gcs_bucket: The Google Cloud Storage bucket to find the objects. (templated)
    :param prefix: Prefix string which filters objects whose name begin with
        this prefix. (templated)
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param dest_aws_conn_id: The destination S3 connection
    :param dest_s3_key: The base S3 key to be used to store the files. (templated)
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.

    :param replace: Whether or not to verify the existence of the files in the
        destination bucket.
        By default is set to False
        If set to True, will upload all the files replacing the existing ones in
        the destination bucket.
        If set to False, will upload only the files that are in the origin but not
        in the destination bucket.
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param s3_acl_policy: Optional The string to specify the canned ACL policy for the
        object to be uploaded in S3
    :param keep_directory_structure: (Optional) When set to False the path of the file
         on the bucket is recreated within path passed in dest_s3_key.
    :param match_glob: (Optional) filters objects based on the glob pattern given by the string
        (e.g, ``'**/*/.json'``)
    :param gcp_user_project: (Optional) The identifier of the Google Cloud project to bill for this request.
        Required for Requester Pays buckets.
    """

    template_fields: Sequence[str] = (
        "gcs_bucket",
        "prefix",
        "dest_s3_key",
        "google_impersonation_chain",
        "gcp_user_project",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        gcs_bucket: str,
        prefix: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        dest_aws_conn_id: str | None = "aws_default",
        dest_s3_key: str,
        dest_verify: str | bool | None = None,
        replace: bool = False,
        google_impersonation_chain: str | Sequence[str] | None = None,
        dest_s3_extra_args: dict | None = None,
        s3_acl_policy: str | None = None,
        keep_directory_structure: bool = True,
        match_glob: str | None = None,
        gcp_user_project: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcs_bucket = gcs_bucket
        self.prefix = prefix
        self.gcp_conn_id = gcp_conn_id
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_s3_key = dest_s3_key
        self.dest_verify = dest_verify
        self.replace = replace
        self.google_impersonation_chain = google_impersonation_chain
        self.dest_s3_extra_args = dest_s3_extra_args or {}
        self.s3_acl_policy = s3_acl_policy
        self.keep_directory_structure = keep_directory_structure
        try:
            from airflow.providers.google import __version__

            if Version(__version__) >= Version("10.3.0"):
                self.__is_match_glob_supported = True
            else:
                self.__is_match_glob_supported = False
        except ImportError:  # __version__ was added in 10.1.0, so this means it's < 10.3.0
            self.__is_match_glob_supported = False
        if not self.__is_match_glob_supported and match_glob:
            raise AirflowException(
                "The 'match_glob' parameter requires 'apache-airflow-providers-google>=10.3.0'."
            )
        self.match_glob = match_glob
        self.gcp_user_project = gcp_user_project

    def execute(self, context: Context) -> list[str]:
        # list all files in an Google Cloud Storage bucket
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.google_impersonation_chain,
        )

        self.log.info(
            "Getting list of the files. Bucket: %s; Prefix: %s",
            self.gcs_bucket,
            self.prefix,
        )

        list_kwargs = {
            "bucket_name": self.gcs_bucket,
            "prefix": self.prefix,
            "user_project": self.gcp_user_project,
        }
        if self.__is_match_glob_supported:
            list_kwargs["match_glob"] = self.match_glob

        gcs_files = gcs_hook.list(**list_kwargs)  # type: ignore

        s3_hook = S3Hook(
            aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify, extra_args=self.dest_s3_extra_args
        )

        if not self.keep_directory_structure and self.prefix:
            self.dest_s3_key = os.path.join(self.dest_s3_key, self.prefix)

        if not self.replace:
            # if we are not replacing -> list all files in the S3 bucket
            # and only keep those files which are present in
            # Google Cloud Storage and not in S3
            bucket_name, prefix = S3Hook.parse_s3_url(self.dest_s3_key)
            # if prefix is empty, do not add "/" at end since it would
            # filter all the objects (return empty list) instead of empty
            # prefix returning all the objects
            if prefix:
                prefix = prefix.rstrip("/") + "/"
            # look for the bucket and the prefix to avoid look into
            # parent directories/keys
            existing_files = s3_hook.list_keys(bucket_name, prefix=prefix)
            # in case that no files exists, return an empty array to avoid errors
            existing_files = existing_files or []
            # remove the prefix for the existing files to allow the match
            existing_files = [file.replace(prefix, "", 1) for file in existing_files]
            gcs_files = list(set(gcs_files) - set(existing_files))

        if gcs_files:
            for file in gcs_files:
                with gcs_hook.provide_file(
                    object_name=file, bucket_name=str(self.gcs_bucket), user_project=self.gcp_user_project
                ) as local_tmp_file:
                    dest_key = os.path.join(self.dest_s3_key, file)
                    self.log.info("Saving file to %s", dest_key)
                    s3_hook.load_file(
                        filename=local_tmp_file.name,
                        key=dest_key,
                        replace=self.replace,
                        acl_policy=self.s3_acl_policy,
                    )
            self.log.info("All done, uploaded %d files to S3", len(gcs_files))
        else:
            self.log.info("In sync, no files needed to be uploaded to S3")

        return gcs_files
