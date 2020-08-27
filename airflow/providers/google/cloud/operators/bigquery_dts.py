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
This module contains Google BigQuery Data Transfer Service operators.
"""
from typing import Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.protobuf.json_format import MessageToDict

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery_dts import BiqQueryDataTransferServiceHook, get_object_id
from airflow.utils.decorators import apply_defaults


class BigQueryCreateDataTransferOperator(BaseOperator):
    """
    Creates a new data transfer configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateDataTransferOperator`

    :param transfer_config: Data transfer configuration to create.
    :type transfer_config: dict
    :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param authorization_code: authorization code to use with this transfer configuration.
        This is required if new credentials are needed.
    :type authorization_code: Optional[str]
    :param retry: A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "transfer_config",
        "project_id",
        "authorization_code",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        transfer_config: dict,
        project_id: Optional[str] = None,
        authorization_code: Optional[str] = None,
        retry: Retry = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id="google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.transfer_config = transfer_config
        self.authorization_code = authorization_code
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = BiqQueryDataTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info("Creating DTS transfer config")
        response = hook.create_transfer_config(
            project_id=self.project_id,
            transfer_config=self.transfer_config,
            authorization_code=self.authorization_code,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(response)
        self.log.info("Created DTS transfer config %s", get_object_id(result))
        self.xcom_push(context, key="transfer_config_id", value=get_object_id(result))
        return result


class BigQueryDeleteDataTransferConfigOperator(BaseOperator):
    """
    Deletes transfer configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDeleteDataTransferConfigOperator`

    :param transfer_config_id: Id of transfer config to be used.
    :type transfer_config_id: str
    :param project_id: The BigQuery project id where the transfer configuration should be
        created. If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param retry: A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "transfer_config_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        transfer_config_id: str,
        project_id: Optional[str] = None,
        retry: Retry = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id="google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.transfer_config_id = transfer_config_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = BiqQueryDataTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        hook.delete_transfer_config(
            transfer_config_id=self.transfer_config_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class BigQueryDataTransferServiceStartTransferRunsOperator(BaseOperator):
    """
    Start manual transfer runs to be executed now with schedule_time equal
    to current time. The transfer runs can be created for a time range where
    the run_time is between start_time (inclusive) and end_time
    (exclusive), or for a specific run_time.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDataTransferServiceStartTransferRunsOperator`

    :param transfer_config_id: Id of transfer config to be used.
    :type transfer_config_id: str
    :param requested_time_range: Time range for the transfer runs that should be started.
        If a dict is provided, it must be of the same form as the protobuf
        message `~google.cloud.bigquery_datatransfer_v1.types.TimeRange`
    :type requested_time_range: Union[dict, ~google.cloud.bigquery_datatransfer_v1.types.TimeRange]
    :param requested_run_time: Specific run_time for a transfer run to be started. The
        requested_run_time must not be in the future.  If a dict is provided, it
        must be of the same form as the protobuf message
        `~google.cloud.bigquery_datatransfer_v1.types.Timestamp`
    :type requested_run_time: Union[dict, ~google.cloud.bigquery_datatransfer_v1.types.Timestamp]
    :param project_id: The BigQuery project id where the transfer configuration should be
        created. If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param retry: A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param timeout: The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "transfer_config_id",
        "project_id",
        "requested_time_range",
        "requested_run_time",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        transfer_config_id: str,
        project_id: Optional[str] = None,
        requested_time_range: Optional[dict] = None,
        requested_run_time: Optional[dict] = None,
        retry: Retry = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id="google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.transfer_config_id = transfer_config_id
        self.requested_time_range = requested_time_range
        self.requested_run_time = requested_run_time
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = BiqQueryDataTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info('Submitting manual transfer for %s', self.transfer_config_id)
        response = hook.start_manual_transfer_runs(
            transfer_config_id=self.transfer_config_id,
            requested_time_range=self.requested_time_range,
            requested_run_time=self.requested_run_time,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(response)
        run_id = None
        if 'runs' in result:
            run_id = get_object_id(result['runs'][0])
            self.xcom_push(context, key="run_id", value=run_id)
        self.log.info('Transfer run %s submitted successfully.', run_id)
        return result
