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
This module contains GCP Stackdriver operators.
"""

import json
from typing import Any, Dict, Optional, Sequence, Union

from google.api_core.exceptions import InvalidArgument
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud import monitoring_v3
from google.protobuf.json_format import MessageToDict, MessageToJson, Parse
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class StackdriverHook(GoogleBaseHook):
    """
    Stackdriver Hook for connecting with GCP Stackdriver
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
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
        project_id: str,
        format_: Optional[str] = None,
        filter_: Optional[str] = None,
        order_by: Optional[str] = None,
        page_size: Optional[int] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None
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
        :type format_: str
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be included in the response.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type filter_: str
        :param order_by: A comma-separated list of fields by which to sort the result.
            Supports the same set of field references as the ``filter`` field. Entries
            can be prefixed with a minus sign to sort by the field in descending order.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type order_by: str
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed per-
            resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number
            of resources in a page.
        :type page_size: int
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        :param project_id: The project to fetch alerts from.
        :type project_id: str
        """
        client = self._get_policy_client()
        policies_ = client.list_alert_policies(
            name='projects/{project_id}'.format(project_id=project_id),
            filter_=filter_,
            order_by=order_by,
            page_size=page_size,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if format_ == "dict":
            return [MessageToDict(policy) for policy in policies_]
        elif format_ == "json":
            return [MessageToJson(policy) for policy in policies_]
        else:
            return policies_

    @GoogleBaseHook.fallback_to_default_project_id
    def _toggle_policy_status(
        self,
        new_state: bool,
        project_id: str,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None
    ):
        client = self._get_policy_client()
        policies_ = self.list_alert_policies(project_id=project_id, filter_=filter_)
        for policy in policies_:
            if policy.enabled.value != bool(new_state):
                policy.enabled.value = bool(new_state)
                mask = monitoring_v3.types.field_mask_pb2.FieldMask()
                mask.paths.append('enabled')  # pylint: disable=no-member
                client.update_alert_policy(
                    alert_policy=policy,
                    update_mask=mask,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata
                )

    @GoogleBaseHook.fallback_to_default_project_id
    def enable_alert_policies(
        self,
        project_id: str,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        """
        Enables one or more disabled alerting policies identified by filter
        parameter. Inoperative in case the policy is already enabled.

        :param project_id: The project in which alert needs to be enabled.
        :type project_id: str
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be enabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type filter_: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """
        self._toggle_policy_status(
            new_state=True,
            project_id=project_id,
            filter_=filter_,
            retry=retry,
            timeout=timeout,
            metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def disable_alert_policies(
        self,
        project_id: str,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        """
        Disables one or more enabled alerting policies identified by filter
        parameter. Inoperative in case the policy is already disabled.

        :param project_id: The project in which alert needs to be disabled.
        :type project_id: str
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be disabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type filter_: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """
        self._toggle_policy_status(
            filter_=filter_,
            project_id=project_id,
            new_state=False,
            retry=retry,
            timeout=timeout,
            metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def upsert_alert(
        self,
        alerts: str,
        project_id: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        """
         Creates a new alert or updates an existing policy identified
         the name field in the alerts parameter.

        :param project_id: The project in which alert needs to be created/updated.
        :type project_id: str
        :param alerts: A JSON string or file that specifies all the alerts that needs
             to be either created or updated. For more details, see
             https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alertPolicies#AlertPolicy.
             (templated)
        :type alerts: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """
        policy_client = self._get_policy_client()
        channel_client = self._get_channel_client()

        record = json.loads(alerts)
        existing_policies = [policy['name'] for policy in
                             self.list_alert_policies(project_id=project_id, format_='dict')]
        existing_channels = [channel['name'] for channel in
                             self.list_notification_channels(project_id=project_id, format_='dict')]
        policies_ = []
        channels = []

        for channel in record["channels"]:
            channel_json = json.dumps(channel)
            channels.append(Parse(channel_json, monitoring_v3.types.notification_pb2.NotificationChannel()))
        for policy in record["policies"]:
            policy_json = json.dumps(policy)
            policies_.append(Parse(policy_json, monitoring_v3.types.alert_pb2.AlertPolicy()))

        channel_name_map = {}

        for channel in channels:
            channel.verification_status = monitoring_v3.enums.NotificationChannel. \
                VerificationStatus.VERIFICATION_STATUS_UNSPECIFIED

            if channel.name in existing_channels:
                channel_client.update_notification_channel(
                    notification_channel=channel,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata
                )
            else:
                old_name = channel.name
                channel.ClearField('name')
                new_channel = channel_client.create_notification_channel(
                    name='projects/{project_id}'.format(project_id=project_id),
                    notification_channel=channel,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata
                )
                channel_name_map[old_name] = new_channel.name

        for policy in policies_:
            policy.ClearField('creation_record')
            policy.ClearField('mutation_record')

            for i, channel in enumerate(policy.notification_channels):
                new_channel = channel_name_map.get(channel)
                if new_channel:
                    policy.notification_channels[i] = new_channel

            if policy.name in existing_policies:
                try:
                    policy_client.update_alert_policy(
                        alert_policy=policy,
                        retry=retry,
                        timeout=timeout,
                        metadata=metadata
                    )
                except InvalidArgument:
                    pass
            else:
                policy.ClearField('name')
                for condition in policy.conditions:
                    condition.ClearField('name')
                policy_client.create_alert_policy(
                    name='projects/{project_id}'.format(project_id=project_id),
                    alert_policy=policy,
                    retry=retry,
                    timeout=timeout,
                    metadata=None
                )

    def delete_alert_policy(
        self,
        name: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        """
        Deletes an alerting policy.

        :param name: The alerting policy to delete. The format is:
                         ``projects/[PROJECT_ID]/alertPolicies/[ALERT_POLICY_ID]``.
        :type name: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """

        policy_client = self._get_policy_client()
        try:
            policy_client.delete_alert_policy(
                name=name,
                retry=retry,
                timeout=timeout,
                metadata=metadata
            )
        except HttpError as err:
            raise AirflowException(
                'Delete alerting policy failed. Error was {}'.format(err.content)
            )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_notification_channels(
        self,
        project_id: str,
        format_: Optional[str] = None,
        filter_: Optional[str] = None,
        order_by: Optional[str] = None,
        page_size: Optional[int] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[str] = DEFAULT,
        metadata: Optional[str] = None,
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
        :type format_: str
        :param filter_:  If provided, this field specifies the criteria that
            must be met by notification channels to be included in the response.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type filter_: str
        :param order_by: A comma-separated list of fields by which to sort the result.
            Supports the same set of field references as the ``filter`` field. Entries
            can be prefixed with a minus sign to sort by the field in descending order.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type order_by: str
        :param page_size: The maximum number of resources contained in the
            underlying API response. If page streaming is performed per-
            resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number
            of resources in a page.
        :type page_size: int
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        :param project_id: The project to fetch notification channels from.
        :type project_id: str
        """

        client = self._get_channel_client()
        channels = client.list_notification_channels(
            name='projects/{project_id}'.format(project_id=project_id),
            filter_=filter_,
            order_by=order_by,
            page_size=page_size,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if format_ == "dict":
            return [MessageToDict(channel) for channel in channels]
        elif format_ == "json":
            return [MessageToJson(channel) for channel in channels]
        else:
            return channels

    @GoogleBaseHook.fallback_to_default_project_id
    def _toggle_channel_status(
        self,
        new_state: bool,
        project_id: str,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[str] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        client = self._get_channel_client()
        channels = client.list_notification_channels(
            name='projects/{project_id}'.format(project_id=project_id),
            filter_=filter_
        )
        for channel in channels:
            if channel.enabled.value != bool(new_state):
                channel.enabled.value = bool(new_state)
                mask = monitoring_v3.types.field_mask_pb2.FieldMask()
                mask.paths.append('enabled')  # pylint: disable=no-member
                client.update_notification_channel(
                    notification_channel=channel,
                    update_mask=mask,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata
                )

    @GoogleBaseHook.fallback_to_default_project_id
    def enable_notification_channels(
        self,
        project_id: str,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[str] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        """
        Enables one or more disabled alerting policies identified by filter
        parameter. Inoperative in case the policy is already enabled.

        :param project_id: The project in which notification channels needs to be enabled.
        :type project_id: str
        :param filter_:  If provided, this field specifies the criteria that
            must be met by notification channels to be enabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type filter_: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """

        self._toggle_channel_status(
            project_id=project_id,
            filter_=filter_,
            new_state=True,
            retry=retry,
            timeout=timeout,
            metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def disable_notification_channels(
        self,
        project_id: str,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[str] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        """
        Disables one or more enabled notification channels identified by filter
        parameter. Inoperative in case the policy is already disabled.

        :param project_id: The project in which notification channels needs to be enabled.
        :type project_id: str
        :param filter_:  If provided, this field specifies the criteria that
            must be met by alert policies to be disabled.
            For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
        :type filter_: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """

        self._toggle_channel_status(
            filter_=filter_,
            project_id=project_id,
            new_state=False,
            retry=retry,
            timeout=timeout,
            metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def upsert_channel(
        self,
        channels: str,
        project_id: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None
    ) -> Dict:
        """
        Creates a new notification or updates an existing notification channel
        identified the name field in the alerts parameter.

        :param channels: A JSON string or file that specifies all the alerts that needs
            to be either created or updated. For more details, see
            https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.notificationChannels.
            (templated)
        :type channels: str
        :param project_id: The project in which notification channels needs to be created/updated.
        :type project_id: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """

        channel_client = self._get_channel_client()

        record = json.loads(channels)
        existing_channels = [channel["name"] for channel in
                             self.list_notification_channels(project_id=project_id, format_="dict")]
        channels_list = []
        channel_name_map = {}

        for channel in record["channels"]:
            channel_json = json.dumps(channel)
            channels_list.append(
                Parse(channel_json, monitoring_v3.types.notification_pb2.NotificationChannel())
            )

        for channel in channels_list:
            channel.verification_status = monitoring_v3.enums.NotificationChannel. \
                VerificationStatus.VERIFICATION_STATUS_UNSPECIFIED

            if channel.name in existing_channels:
                channel_client.update_notification_channel(
                    notification_channel=channel,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata
                )
            else:
                old_name = channel.name
                channel.ClearField('name')
                new_channel = channel_client.create_notification_channel(
                    name='projects/{project_id}'.format(project_id=project_id),
                    notification_channel=channel,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata
                )
                channel_name_map[old_name] = new_channel.name

        return channel_name_map

    def delete_notification_channel(
        self,
        name: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[str] = DEFAULT,
        metadata: Optional[str] = None
    ) -> None:
        """
        Deletes a notification channel.

        :param name: The alerting policy to delete. The format is:
                         ``projects/[PROJECT_ID]/notificationChannels/[CHANNEL_ID]``.
        :type name: str
        :param retry: A retry object used to retry requests. If ``None`` is
            specified, requests will be retried using a default configuration.
        :type retry: str
        :param timeout: The amount of time, in seconds, to wait
            for the request to complete. Note that if ``retry`` is
            specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: str
        """

        channel_client = self._get_channel_client()
        try:
            channel_client.delete_notification_channel(
                name=name,
                retry=retry,
                timeout=timeout,
                metadata=metadata
            )
        except HttpError as err:
            raise AirflowException(
                'Delete notification channel failed. Error was {}'.format(err.content)
            )
