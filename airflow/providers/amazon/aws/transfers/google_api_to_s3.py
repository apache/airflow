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
"""This module allows you to transfer data from any Google API endpoint into a S3 Bucket."""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.models.xcom import MAX_XCOM_SIZE, XCOM_RETURN_KEY
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.common.hooks.discovery_api import GoogleDiscoveryApiHook

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
    from airflow.utils.context import Context


class GoogleApiToS3Operator(BaseOperator):
    """
    Basic class for transferring data from a Google API endpoint into a S3 Bucket.

    This discovery-based operator use
    :class:`~airflow.providers.google.common.hooks.discovery_api.GoogleDiscoveryApiHook` to communicate
    with Google Services via the
    `Google API Python Client <https://github.com/googleapis/google-api-python-client>`__.
    Please note that this library is in maintenance mode hence it won't fully support Google Cloud in
    the future.
    Therefore it is recommended that you use the custom Google Cloud Service Operators for working
    with the Google Cloud Platform.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleApiToS3Operator`

    :param google_api_service_name: The specific API service that is being requested.
    :param google_api_service_version: The version of the API that is being requested.
    :param google_api_endpoint_path: The client libraries path to the api call's executing method.
        For example: 'analyticsreporting.reports.batchGet'

        .. note:: See https://developers.google.com/apis-explorer
            for more information on which methods are available.

    :param google_api_endpoint_params: The params to control the corresponding endpoint result.
    :param s3_destination_key: The url where to put the data retrieved from the endpoint in S3.

        .. note See https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html
            for valid url formats.

    :param google_api_response_via_xcom: Can be set to expose the google api response to xcom.
    :param google_api_endpoint_params_via_xcom: If set to a value this value will be used as a key
        for pulling from xcom and updating the google api endpoint params.
    :param google_api_endpoint_params_via_xcom_task_ids: Task ids to filter xcom by.
    :param google_api_pagination: If set to True Pagination will be enabled for this request
        to retrieve all data.

        .. note:: This means the response will be a list of responses.

    :param google_api_num_retries: Define the number of retries for the Google API requests being made
        if it fails.
    :param s3_overwrite: Specifies whether the s3 file will be overwritten if exists.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param aws_conn_id: The connection id specifying the authentication information for the S3 Bucket.
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "google_api_endpoint_params",
        "s3_destination_key",
        "google_impersonation_chain",
        "gcp_conn_id",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#cc181e"

    def __init__(
        self,
        *,
        google_api_service_name: str,
        google_api_service_version: str,
        google_api_endpoint_path: str,
        google_api_endpoint_params: dict,
        s3_destination_key: str,
        google_api_response_via_xcom: str | None = None,
        google_api_endpoint_params_via_xcom: str | None = None,
        google_api_endpoint_params_via_xcom_task_ids: str | None = None,
        google_api_pagination: bool = False,
        google_api_num_retries: int = 0,
        s3_overwrite: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        aws_conn_id: str | None = "aws_default",
        google_impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.google_api_service_name = google_api_service_name
        self.google_api_service_version = google_api_service_version
        self.google_api_endpoint_path = google_api_endpoint_path
        self.google_api_endpoint_params = google_api_endpoint_params
        self.s3_destination_key = s3_destination_key
        self.google_api_response_via_xcom = google_api_response_via_xcom
        self.google_api_endpoint_params_via_xcom = google_api_endpoint_params_via_xcom
        self.google_api_endpoint_params_via_xcom_task_ids = google_api_endpoint_params_via_xcom_task_ids
        self.google_api_pagination = google_api_pagination
        self.google_api_num_retries = google_api_num_retries
        self.s3_overwrite = s3_overwrite
        self.gcp_conn_id = gcp_conn_id
        self.aws_conn_id = aws_conn_id
        self.google_impersonation_chain = google_impersonation_chain

    def execute(self, context: Context) -> None:
        """
        Transfers Google APIs json data to S3.

        :param context: The context that is being provided when executing.
        """
        self.log.info("Transferring data from %s to s3", self.google_api_service_name)

        if self.google_api_endpoint_params_via_xcom:
            self._update_google_api_endpoint_params_via_xcom(context["task_instance"])

        data = self._retrieve_data_from_google_api()

        self._load_data_to_s3(data)

        if self.google_api_response_via_xcom:
            self._expose_google_api_response_via_xcom(context["task_instance"], data)

    def _retrieve_data_from_google_api(self) -> dict:
        google_discovery_api_hook = GoogleDiscoveryApiHook(
            gcp_conn_id=self.gcp_conn_id,
            api_service_name=self.google_api_service_name,
            api_version=self.google_api_service_version,
            impersonation_chain=self.google_impersonation_chain,
        )
        return google_discovery_api_hook.query(
            endpoint=self.google_api_endpoint_path,
            data=self.google_api_endpoint_params,
            paginate=self.google_api_pagination,
            num_retries=self.google_api_num_retries,
        )

    def _load_data_to_s3(self, data: dict) -> None:
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_string(
            string_data=json.dumps(data),
            bucket_name=S3Hook.parse_s3_url(self.s3_destination_key)[0],
            key=S3Hook.parse_s3_url(self.s3_destination_key)[1],
            replace=self.s3_overwrite,
        )

    def _update_google_api_endpoint_params_via_xcom(
        self, task_instance: TaskInstance | TaskInstancePydantic
    ) -> None:
        if self.google_api_endpoint_params_via_xcom:
            google_api_endpoint_params = task_instance.xcom_pull(
                task_ids=self.google_api_endpoint_params_via_xcom_task_ids,
                key=self.google_api_endpoint_params_via_xcom,
            )
            self.google_api_endpoint_params.update(google_api_endpoint_params)

    def _expose_google_api_response_via_xcom(
        self, task_instance: TaskInstance | TaskInstancePydantic, data: dict
    ) -> None:
        if sys.getsizeof(data) < MAX_XCOM_SIZE:
            task_instance.xcom_push(key=self.google_api_response_via_xcom or XCOM_RETURN_KEY, value=data)
        else:
            raise RuntimeError("The size of the downloaded data is too large to push to XCom!")
