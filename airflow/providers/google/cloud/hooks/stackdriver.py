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
"""This module contains Google Cloud Stackdriver operators."""
from __future__ import annotations

import json
import warnings
from typing import Any, Sequence

from google.api_core.exceptions import InvalidArgument
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import AlertPolicy, NotificationChannel
from google.protobuf.field_mask_pb2 import FieldMask
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


class StackdriverHook(GoogleBaseHook):
    """Stackdriver Hook for connecting with Google Cloud Stackdriver"""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._policy_client = None
        self._channel_client = None

    def _get_policy_client(self):
        if not self._policy_client:
            self._policy_client = monitoring_v3.AlertPolicyServiceClient()
        return self._policy_client

    def _get_channel_client(self):
        if not self._channel_client:
            self._channel_client = monitoring_v3.NotificationChannelServiceClient()
        return self._channel_client

    @GoogleBaseHook.fallback_to_default_project_id
    def list_alert_policies(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        format_: str | None = None,
        filter_: str | None = None,
        order_by: str | None = None,
        page_size: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Fetches all the Alert Policies identified by the filter passed as
        filter parameter. The desired return type can be specified by the
        format parameter, the supported formats are "dict", "json" and None
        which returns python dictionary, stringified JSON and protobuf
        respectively.

        :param format_: (Optional) Desired output format of the result. The
            supported formats are "dict", "json" and None which returns
            python dictionary, stringified JSON and protobuf respectively.
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be included in the response.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param order_by: A comma-separated list of fields by which to sort the result.
            Supports the same set of field references as the ``filter`` field. Entries
            can be prefixed with a minus sign to sort by the field in descending order.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed per-
            resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number
            of resources in a page.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :param project_id: The project to fetch alerts from.
        """
        client = self._get_policy_client()
        policies_ = client.list_alert_policies(
            request={
                "name": f"projects/{project_id}",
                "filter": filter_,
                "order_by": order_by,
                "page_size": page_size,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if format_ == "dict":
            return [AlertPolicy.to_dict(policy) for policy in policies_]
        elif format_ == "json":
            return [AlertPolicy.to_jsoon(policy) for policy in policies_]
        else:
            return policies_

    @GoogleBaseHook.fallback_to_default_project_id
    def _toggle_policy_status(
        self,
        new_state: bool,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        client = self._get_policy_client()
        policies_ = self.list_alert_policies(project_id=project_id, filter_=filter_)
        for policy in policies_:
            if policy.enabled != bool(new_state):
                policy.enabled = bool(new_state)
                mask = FieldMask(paths=["enabled"])
                client.update_alert_policy(
                    request={"alert_policy": policy, "update_mask": mask},
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )

    @GoogleBaseHook.fallback_to_default_project_id
    def enable_alert_policies(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Enables one or more disabled alerting policies identified by filter
        parameter. Inoperative in case the policy is already enabled.

        :param project_id: The project in which alert needs to be enabled.
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be enabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        self._toggle_policy_status(
            new_state=True,
            project_id=project_id,
            filter_=filter_,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def disable_alert_policies(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Disables one or more enabled alerting policies identified by filter
        parameter. Inoperative in case the policy is already disabled.

        :param project_id: The project in which alert needs to be disabled.
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be disabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        self._toggle_policy_status(
            filter_=filter_,
            project_id=project_id,
            new_state=False,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def upsert_alert(
        self,
        alerts: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
         Creates a new alert or updates an existing policy identified
         the name field in the alerts parameter.

        :param project_id: The project in which alert needs to be created/updated.
        :param alerts: A JSON string or file that specifies all the alerts that needs
             to be either created or updated. For more details, see
             https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alertPolicies#AlertPolicy.
             (templated)
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        policy_client = self._get_policy_client()
        channel_client = self._get_channel_client()

        record = json.loads(alerts)
        existing_policies = [
            policy["name"] for policy in self.list_alert_policies(project_id=project_id, format_="dict")
        ]
        existing_channels = [
            channel["name"]
            for channel in self.list_notification_channels(project_id=project_id, format_="dict")
        ]
        policies_ = []
        channels = []
        for channel in record.get("channels", []):
            channels.append(NotificationChannel(**channel))
        for policy in record.get("policies", []):
            policies_.append(AlertPolicy(**policy))

        channel_name_map = {}

        for channel in channels:
            channel.verification_status = (
                monitoring_v3.NotificationChannel.VerificationStatus.VERIFICATION_STATUS_UNSPECIFIED
            )

            if channel.name in existing_channels:
                channel_client.update_notification_channel(
                    request={"notification_channel": channel},
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
            else:
                old_name = channel.name
                channel.name = None
                new_channel = channel_client.create_notification_channel(
                    request={"name": f"projects/{project_id}", "notification_channel": channel},
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
                channel_name_map[old_name] = new_channel.name

        for policy in policies_:
            policy.creation_record = None
            policy.mutation_record = None

            for i, channel in enumerate(policy.notification_channels):
                new_channel = channel_name_map.get(channel)
                if new_channel:
                    policy.notification_channels[i] = new_channel

            if policy.name in existing_policies:
                try:
                    policy_client.update_alert_policy(
                        request={"alert_policy": policy},
                        retry=retry,
                        timeout=timeout,
                        metadata=metadata,
                    )
                except InvalidArgument:
                    pass
            else:
                policy.name = None
                for condition in policy.conditions:
                    condition.name = None
                policy_client.create_alert_policy(
                    request={"name": f"projects/{project_id}", "alert_policy": policy},
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )

    def delete_alert_policy(
        self,
        name: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes an alerting policy.

        :param name: The alerting policy to delete. The format is:
                         ``projects/[PROJECT_ID]/alertPolicies/[ALERT_POLICY_ID]``.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        policy_client = self._get_policy_client()
        try:
            policy_client.delete_alert_policy(
                request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ()
            )
        except HttpError as err:
            raise AirflowException(f"Delete alerting policy failed. Error was {err.content}")

    @GoogleBaseHook.fallback_to_default_project_id
    def list_notification_channels(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        format_: str | None = None,
        filter_: str | None = None,
        order_by: str | None = None,
        page_size: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Fetches all the Notification Channels identified by the filter passed as
        filter parameter. The desired return type can be specified by the
        format parameter, the supported formats are "dict", "json" and None
        which returns python dictionary, stringified JSON and protobuf
        respectively.

        :param format_: (Optional) Desired output format of the result. The
            supported formats are "dict", "json" and None which returns
            python dictionary, stringified JSON and protobuf respectively.
        :param filter_:  If provided, this field specifies the criteria that
            must be met by notification channels to be included in the response.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param order_by: A comma-separated list of fields by which to sort the result.
            Supports the same set of field references as the ``filter`` field. Entries
            can be prefixed with a minus sign to sort by the field in descending order.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed per-
            resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number
            of resources in a page.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :param project_id: The project to fetch notification channels from.
        """
        client = self._get_channel_client()
        channels = client.list_notification_channels(
            request={
                "name": f"projects/{project_id}",
                "filter": filter_,
                "order_by": order_by,
                "page_size": page_size,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if format_ == "dict":
            return [NotificationChannel.to_dict(channel) for channel in channels]
        elif format_ == "json":
            return [NotificationChannel.to_json(channel) for channel in channels]
        else:
            return channels

    @GoogleBaseHook.fallback_to_default_project_id
    def _toggle_channel_status(
        self,
        new_state: bool,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        client = self._get_channel_client()
        channels = client.list_notification_channels(
            request={"name": f"projects/{project_id}", "filter": filter_}
        )
        for channel in channels:
            if channel.enabled != bool(new_state):
                channel.enabled = bool(new_state)
                mask = FieldMask(paths=["enabled"])
                client.update_notification_channel(
                    request={"notification_channel": channel, "update_mask": mask},
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )

    @GoogleBaseHook.fallback_to_default_project_id
    def enable_notification_channels(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Enables one or more disabled alerting policies identified by filter
        parameter. Inoperative in case the policy is already enabled.

        :param project_id: The project in which notification channels needs to be enabled.
        :param filter_:  If provided, this field specifies the criteria that
            must be met by notification channels to be enabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        self._toggle_channel_status(
            project_id=project_id,
            filter_=filter_,
            new_state=True,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def disable_notification_channels(
        self,
        project_id: str,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Disables one or more enabled notification channels identified by filter
        parameter. Inoperative in case the policy is already disabled.

        :param project_id: The project in which notification channels needs to be enabled.
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be disabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        self._toggle_channel_status(
            filter_=filter_,
            project_id=project_id,
            new_state=False,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def upsert_channel(
        self,
        channels: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> dict:
        """
        Creates a new notification or updates an existing notification channel
        identified the name field in the alerts parameter.

        :param channels: A JSON string or file that specifies all the alerts that needs
            to be either created or updated. For more details, see
            https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.notificationChannels.
            (templated)
        :param project_id: The project in which notification channels needs to be created/updated.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        channel_client = self._get_channel_client()

        record = json.loads(channels)
        existing_channels = [
            channel["name"]
            for channel in self.list_notification_channels(project_id=project_id, format_="dict")
        ]
        channels_list = []
        channel_name_map = {}

        for channel in record["channels"]:
            channels_list.append(NotificationChannel(**channel))

        for channel in channels_list:
            channel.verification_status = (
                monitoring_v3.NotificationChannel.VerificationStatus.VERIFICATION_STATUS_UNSPECIFIED
            )

            if channel.name in existing_channels:
                channel_client.update_notification_channel(
                    request={"notification_channel": channel},
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
            else:
                old_name = channel.name
                channel.name = None
                new_channel = channel_client.create_notification_channel(
                    request={"name": f"projects/{project_id}", "notification_channel": channel},
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
                channel_name_map[old_name] = new_channel.name

        return channel_name_map

    def delete_notification_channel(
        self,
        name: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes a notification channel.

        :param name: The alerting policy to delete. The format is:
                         ``projects/[PROJECT_ID]/notificationChannels/[CHANNEL_ID]``.
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        channel_client = self._get_channel_client()
        try:
            channel_client.delete_notification_channel(
                request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ()
            )
        except HttpError as err:
            raise AirflowException(f"Delete notification channel failed. Error was {err.content}")
