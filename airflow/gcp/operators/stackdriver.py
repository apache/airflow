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

from google.api_core.gapic_v1.method import DEFAULT

from airflow.gcp.hooks.stackdriver import StackdriverHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# Alerting Operators
# List alert operators
class StackdriverListAlertPoliciesOperator(BaseOperator):
    """
    Fetches all the Alert Policies identified by the filter passed as
    filter_ parameter. The desired return type can be specified by the
    format_ parameter, the supported formats are "dict", "json" and None
    which returns python dictionary, stringified JSON and protobuf
    respectively.
    :param format_: (Optional) Desired output format of the result. The
        supported formats are "dict", "json" and None which returns
        python dictionary, stringified JSON and protobuf respectively.
    :type format_: str
    :param filter_:  If provided, this field specifies the criteria that
        must be met by alert policies to be included in the response.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
    :type filter_: str
    :param order_by: A comma-separated list of fields by which to sort the result.
        Supports the same set of field references as the ``filter`` field. Entries
        can be prefixed with a minus sign to sort by the field in descending order.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('filter_',)
    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        format_=None,
        filter_=None,
        order_by=None,
        page_size=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.format_ = format_
        self.filter_ = filter_
        self.order_by = order_by
        self.page_size = page_size
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)

        return self.hook.list_alert_policies(
            project_id=self.project_id,
            format_=self.format_,
            filter_=self.filter_,
            order_by=self.order_by,
            page_size=self.page_size,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


class StackdriverEnableAlertPoliciesOperator(BaseOperator):
    """
    Enables one or more disabled alerting policies identified by filter_
    parameter. Inoperative in case the policy is already enabled.
    :param filter_:  If provided, this field specifies the criteria that
        must be met by alert policies to be enabled.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    ui_color = "#e5ffcc"
    template_fields = ('filter_',)

    @apply_defaults
    def __init__(
        self,
        filter_=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.enable_alert_policies(
            filter_=self.filter_,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


# Disable Alert Operator
class StackdriverDisableAlertPoliciesOperator(BaseOperator):
    """
    Disables one or more enabled alerting policies identified by filter_
    parameter. Inoperative in case the policy is already disabled.
    :param filter_:  If provided, this field specifies the criteria that
        must be met by alert policies to be disabled.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    ui_color = "#e5ffcc"
    template_fields = ('filter_',)

    @apply_defaults
    def __init__(
        self,
        filter_=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.disable_alert_policies(
            filter_=self.filter_,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


# Upsert alert operator
class StackdriverUpsertAlertOperator(BaseOperator):
    """
    Creates a new alert or updates an existing policy identified
    the name field in the alerts parameter.
    :param alerts: A JSON string or file that specifies all the alerts that needs
        to be either created or updated. For more details, see `alert policies
        <https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alertPolicies#AlertPolicy>`__.
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('alerts',)
    template_ext = ('.json',)

    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        alerts,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.alerts = alerts
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.upsert_alert(
            alerts=self.alerts,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


# Delete alert operator
class StackdriverDeleteAlertOperator(BaseOperator):
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('name',)

    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        name,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.name = name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.delete_alert_policy(
            name=self.name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


# Notification Channel Operators
# List notif channels
class StackdriverListNotificationChannelsOperator(BaseOperator):
    """
    Fetches all the Notification Channels identified by the filter passed as
    filter_ parameter. The desired return type can be specified by the
    format_ parameter, the supported formats are "dict", "json" and None
    which returns python dictionary, stringified JSON and protobuf
    respectively.
    :param format_: (Optional) Desired output format of the result. The
        supported formats are "dict", "json" and None which returns
        python dictionary, stringified JSON and protobuf respectively.
    :type format_: str
    :param filter_:  If provided, this field specifies the criteria that
        must be met by notification channels to be included in the response.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
    :type filter_: str
    :param order_by: A comma-separated list of fields by which to sort the result.
        Supports the same set of field references as the ``filter`` field. Entries
        can be prefixed with a minus sign to sort by the field in descending order.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('filter_',)

    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        format_=None,
        filter_=None,
        order_by=None,
        page_size=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.format_ = format_
        self.filter_ = filter_
        self.order_by = order_by
        self.page_size = page_size
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        return self.hook.list_notification_channels(
            format_=self.format_,
            filter_=self.filter_,
            order_by=self.order_by,
            page_size=self.page_size,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


class StackdriverEnableNotificationChannelsOperator(BaseOperator):
    """
    Enables one or more disabled alerting policies identified by filter_
    parameter. Inoperative in case the policy is already enabled.
    :param filter_:  If provided, this field specifies the criteria that
        must be met by notification channels to be enabled.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('filter_',)

    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        filter_=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.enable_notification_status(
            filter_=self.filter_,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


class StackdriverDisableNotificationChannelsOperator(BaseOperator):
    """
    Disables one or more enabled notification channels identified by filter_
    parameter. Inoperative in case the policy is already disabled.
    :param filter_:  If provided, this field specifies the criteria that
        must be met by alert policies to be disabled.
        For more details, see `sorting and filtering
        <https://cloud.google.com/monitoring/api/v3/sorting-and-filtering>`__.
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('filter_',)

    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        filter_=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.disable_alert_policies(
            filter_=self.filter_,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


class StackdriverUpsertNotificationChannelOperator(BaseOperator):
    """
    Creates a new notification or updates an existing notification channel
    identified the name field in the alerts parameter.
    :param channels: A JSON string or file that specifies all the alerts that needs
        to be either created or updated. For more details, see `notification channels
        <https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.notificationChannels>`__.
        (templated)
    :type channels: str
    :param retry: A retry object used to retry requests. If ``None`` is
        specified, requests will be retried using a default configuration.
    :type retry: str
    :param timeout: The amount of time, in seconds, to wait
        for the request to complete. Note that if ``retry`` is
        specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('channels',)
    template_ext = ('.json',)

    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        channels,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.channels = channels
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.upsert_channel(
            channels=self.channels,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )


class StackdriverDeleteNotificationChannel(BaseOperator):
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param project_id: The location used for the operation.
    :type project_id: str
    :param delegate_to: (Optional) The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('name',)

    ui_color = "#e5ffcc"

    @apply_defaults
    def __init__(
        self,
        name,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
        gcp_conn_id='google_cloud_default',
        project_id=None,
        delegate_to=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.name = name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.hook = None

    def execute(self, context):
        if self.hook is None:
            self.hook = StackdriverHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self.hook.delete_notification_channel(
            name=self.name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )
