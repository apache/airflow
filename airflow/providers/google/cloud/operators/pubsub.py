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
"""
This module contains Google PubSub operators.
"""
import warnings
from typing import Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.pubsub_v1.types import Duration, MessageStoragePolicy, PushConfig

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.utils.decorators import apply_defaults


class PubSubTopicCreateOperator(BaseOperator):
    """Create a PubSub topic.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubTopicCreateOperator`

    By default, if the topic already exists, this operator will
    not cause the DAG to fail. ::

        with DAG('successful DAG') as dag:
            (
                dag
                >> PubSubTopicCreateOperator(project='my-project',
                                             topic='my_new_topic')
                >> PubSubTopicCreateOperator(project='my-project',
                                             topic='my_new_topic')
            )

    The operator can be configured to fail if the topic already exists. ::

        with DAG('failing DAG') as dag:
            (
                dag
                >> PubSubTopicCreateOperator(project='my-project',
                                             topic='my_new_topic')
                >> PubSubTopicCreateOperator(project='my-project',
                                             topic='my_new_topic',
                                             fail_if_exists=True)
            )

    Both ``project`` and ``topic`` are templated so you can use
    variables in them.

    :param project_id: Optional, the GCP project ID where the topic will be created.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param topic: the topic to create. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :type topic: str
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request
        must have domain-wide delegation enabled.
    :type delegate_to: str
    :param labels: Client-assigned labels; see
        https://cloud.google.com/pubsub/docs/labels
    :type labels: Dict[str, str]
    :param message_storage_policy: Policy constraining the set
        of Google Cloud Platform regions where messages published to
        the topic may be stored. If not present, then no constraints
        are in effect.
    :type message_storage_policy:
        Union[Dict, google.cloud.pubsub_v1.types.MessageStoragePolicy]
    :param kms_key_name: The resource name of the Cloud KMS CryptoKey
        to be used to protect access to messages published on this topic.
        The expected format is
        ``projects/*/locations/*/keyRings/*/cryptoKeys/*``.
    :type kms_key_name: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]]
    :param project: (Deprecated) the GCP project ID where the topic will be created
    :type project: str
    """
    template_fields = ['project_id', 'topic']
    ui_color = '#0273d4'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
            self,
            topic: str,
            project_id: Optional[str] = None,
            fail_if_exists: bool = False,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            labels: Optional[Dict[str, str]] = None,
            message_storage_policy: Union[Dict, MessageStoragePolicy] = None,
            kms_key_name: Optional[str] = None,
            retry: Optional[Retry] = None,
            timeout: Optional[float] = None,
            metadata: Optional[Sequence[Tuple[str, str]]] = None,
            project: Optional[str] = None,
            *args,
            **kwargs) -> None:

        # To preserve backward compatibility
        # TODO: remove one day
        if project:
            warnings.warn(
                "The project parameter has been deprecated. You should pass "
                "the project_id parameter.", DeprecationWarning, stacklevel=2)
            project_id = project

        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.topic = topic
        self.fail_if_exists = fail_if_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.labels = labels
        self.message_storage_policy = message_storage_policy
        self.kms_key_name = kms_key_name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        self.log.info("Creating topic %s", self.topic)
        hook.create_topic(
            project_id=self.project_id,
            topic=self.topic,
            fail_if_exists=self.fail_if_exists,
            labels=self.labels,
            message_storage_policy=self.message_storage_policy,
            kms_key_name=self.kms_key_name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )
        self.log.info("Created topic %s", self.topic)


class PubSubSubscriptionCreateOperator(BaseOperator):
    """Create a PubSub subscription.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubSubscriptionCreateOperator`

    By default, the subscription will be created in ``topic_project``. If
    ``subscription_project`` is specified and the GCP credentials allow, the
    Subscription can be created in a different project from its topic.

    By default, if the subscription already exists, this operator will
    not cause the DAG to fail. However, the topic must exist in the project. ::

        with DAG('successful DAG') as dag:
            (
                dag
                >> PubSubSubscriptionCreateOperator(
                    topic_project='my-project', topic='my-topic',
                    subscription='my-subscription')
                >> PubSubSubscriptionCreateOperator(
                    topic_project='my-project', topic='my-topic',
                    subscription='my-subscription')
            )

    The operator can be configured to fail if the subscription already exists.
    ::

        with DAG('failing DAG') as dag:
            (
                dag
                >> PubSubSubscriptionCreateOperator(
                    topic_project='my-project', topic='my-topic',
                    subscription='my-subscription')
                >> PubSubSubscriptionCreateOperator(
                    topic_project='my-project', topic='my-topic',
                    subscription='my-subscription', fail_if_exists=True)
            )

    Finally, subscription is not required. If not passed, the operator will
    generated a universally unique identifier for the subscription's name. ::

        with DAG('DAG') as dag:
            (
                dag >> PubSubSubscriptionCreateOperator(
                    topic_project='my-project', topic='my-topic')
            )

    ``topic_project``, ``topic``, ``subscription``, and
    ``subscription`` are templated so you can use variables in them.

    :param project_id: Optional, the GCP project ID where the topic exists.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param topic: the topic to create. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :type topic: str
    :param subscription: the Pub/Sub subscription name. If empty, a random
        name will be generated using the uuid module
    :type subscription: str
    :param subscription_project_id: the GCP project ID where the subscription
        will be created. If empty, ``topic_project`` will be used.
    :type subscription_project_id: str
    :param ack_deadline_secs: Number of seconds that a subscriber has to
        acknowledge each message pulled from the subscription
    :type ack_deadline_secs: int
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request
        must have domain-wide delegation enabled.
    :type delegate_to: str
    :param push_config: If push delivery is used with this subscription,
        this field is used to configure it. An empty ``pushConfig`` signifies
        that the subscriber will pull and ack messages using API methods.
    :type push_config: Union[Dict, google.cloud.pubsub_v1.types.PushConfig]
    :param retain_acked_messages: Indicates whether to retain acknowledged
        messages. If true, then messages are not expunged from the subscription's
        backlog, even if they are acknowledged, until they fall out of the
        ``message_retention_duration`` window. This must be true if you would
        like to Seek to a timestamp.
    :type retain_acked_messages: bool
    :param message_retention_duration: How long to retain unacknowledged messages
        in the subscription's backlog, from the moment a message is published. If
        ``retain_acked_messages`` is true, then this also configures the
        retention of acknowledged messages, and thus configures how far back in
        time a ``Seek`` can be done. Defaults to 7 days. Cannot be more than 7
        days or less than 10 minutes.
    :type message_retention_duration: Union[Dict, google.cloud.pubsub_v1.types.Duration]
    :param labels: Client-assigned labels; see
        https://cloud.google.com/pubsub/docs/labels
    :type labels: Dict[str, str]
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]]
    :param topic_project: (Deprecated) the GCP project ID where the topic exists
    :type topic_project: str
    :param subscription_project: (Deprecated) the GCP project ID where the subscription
        will be created. If empty, ``topic_project`` will be used.
    :type subscription_project: str
    """
    template_fields = ['project_id', 'topic', 'subscription', 'subscription_project_id']
    ui_color = '#0273d4'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
            self,
            topic: str,
            project_id: Optional[str] = None,
            subscription: Optional[str] = None,
            subscription_project_id: Optional[str] = None,
            ack_deadline_secs: int = 10,
            fail_if_exists: bool = False,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            push_config: Optional[Union[Dict, PushConfig]] = None,
            retain_acked_messages: Optional[bool] = None,
            message_retention_duration: Optional[Union[Dict, Duration]] = None,
            labels: Optional[Dict[str, str]] = None,
            retry: Optional[Retry] = None,
            timeout: Optional[float] = None,
            metadata: Optional[Sequence[Tuple[str, str]]] = None,
            topic_project: Optional[str] = None,
            subscription_project: Optional[str] = None,
            *args,
            **kwargs) -> None:

        # To preserve backward compatibility
        # TODO: remove one day
        if topic_project:
            warnings.warn(
                "The topic_project parameter has been deprecated. You should pass "
                "the project_id parameter.", DeprecationWarning, stacklevel=2)
            project_id = topic_project
        if subscription_project:
            warnings.warn(
                "The project_id parameter has been deprecated. You should pass "
                "the subscription_project parameter.", DeprecationWarning, stacklevel=2)
            subscription_project_id = subscription_project

        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.topic = topic
        self.subscription = subscription
        self.subscription_project_id = subscription_project_id
        self.ack_deadline_secs = ack_deadline_secs
        self.fail_if_exists = fail_if_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.push_config = push_config
        self.retain_acked_messages = retain_acked_messages
        self.message_retention_duration = message_retention_duration
        self.labels = labels
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        self.log.info("Creating subscription for topic %s", self.topic)
        result = hook.create_subscription(
            project_id=self.project_id,
            topic=self.topic,
            subscription=self.subscription,
            subscription_project_id=self.subscription_project_id,
            ack_deadline_secs=self.ack_deadline_secs,
            fail_if_exists=self.fail_if_exists,
            push_config=self.push_config,
            retain_acked_messages=self.retain_acked_messages,
            message_retention_duration=self.message_retention_duration,
            labels=self.labels,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )

        self.log.info("Created subscription for topic %s", self.topic)
        return result


class PubSubTopicDeleteOperator(BaseOperator):
    """Delete a PubSub topic.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubTopicDeleteOperator`

    By default, if the topic does not exist, this operator will
    not cause the DAG to fail. ::

        with DAG('successful DAG') as dag:
            (
                dag
                >> PubSubTopicDeleteOperator(project='my-project',
                                             topic='non_existing_topic')
            )

    The operator can be configured to fail if the topic does not exist. ::

        with DAG('failing DAG') as dag:
            (
                dag
                >> PubSubTopicCreateOperator(project='my-project',
                                             topic='non_existing_topic',
                                             fail_if_not_exists=True)
            )

    Both ``project`` and ``topic`` are templated so you can use
    variables in them.

    :param project_id: Optional, the GCP project ID in which to work (templated).
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param topic: the topic to delete. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :type topic: str
    :param fail_if_not_exists: If True and the topic does not exist, fail
        the task
    :type fail_if_not_exists: bool
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request
        must have domain-wide delegation enabled.
    :type delegate_to: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]]
    :param project: (Deprecated) the GCP project ID where the topic will be created
    :type project: str
    """
    template_fields = ['project_id', 'topic']
    ui_color = '#cb4335'

    @apply_defaults
    def __init__(
            self,
            topic: str,
            project_id: Optional[str] = None,
            fail_if_not_exists: bool = False,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            retry: Optional[Retry] = None,
            timeout: Optional[float] = None,
            metadata: Optional[Sequence[Tuple[str, str]]] = None,
            project: Optional[str] = None,
            *args,
            **kwargs) -> None:

        # To preserve backward compatibility
        # TODO: remove one day
        if project:
            warnings.warn(
                "The project parameter has been deprecated. You should pass "
                "the project_id parameter.", DeprecationWarning, stacklevel=2)
            project_id = project

        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.topic = topic
        self.fail_if_not_exists = fail_if_not_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        self.log.info("Deleting topic %s", self.topic)
        hook.delete_topic(
            project_id=self.project_id,
            topic=self.topic,
            fail_if_not_exists=self.fail_if_not_exists,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )
        self.log.info("Deleted topic %s", self.topic)


class PubSubSubscriptionDeleteOperator(BaseOperator):
    """Delete a PubSub subscription.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubSubscriptionDeleteOperator`

    By default, if the subscription does not exist, this operator will
    not cause the DAG to fail. ::

        with DAG('successful DAG') as dag:
            (
                dag
                >> PubSubSubscriptionDeleteOperator(project='my-project',
                                                    subscription='non-existing')
            )

    The operator can be configured to fail if the subscription already exists.

    ::

        with DAG('failing DAG') as dag:
            (
                dag
                >> PubSubSubscriptionDeleteOperator(
                     project='my-project', subscription='non-existing',
                     fail_if_not_exists=True)
            )

    ``project``, and ``subscription`` are templated so you can use
    variables in them.

    :param project_id: Optional, the GCP project ID in which to work (templated).
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param subscription: the subscription to delete. Do not include the
        full subscription path. In other words, instead of
        ``projects/{project}/subscription/{subscription}``, provide only
        ``{subscription}``. (templated)
    :type subscription: str
    :param fail_if_not_exists: If True and the subscription does not exist,
        fail the task
    :type fail_if_not_exists: bool
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request
        must have domain-wide delegation enabled.
    :type delegate_to: str
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :type timeout: float
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]]
    :param project: (Deprecated) the GCP project ID where the topic will be created
    :type project: str
    """
    template_fields = ['project_id', 'subscription']
    ui_color = '#cb4335'

    @apply_defaults
    def __init__(
            self,
            subscription: str,
            project_id: Optional[str] = None,
            fail_if_not_exists: bool = False,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            retry: Optional[Retry] = None,
            timeout: Optional[float] = None,
            metadata: Optional[Sequence[Tuple[str, str]]] = None,
            project: Optional[str] = None,
            *args,
            **kwargs) -> None:

        # To preserve backward compatibility
        # TODO: remove one day
        if project:
            warnings.warn(
                "The project parameter has been deprecated. You should pass "
                "the project_id parameter.", DeprecationWarning, stacklevel=2)
            project_id = project

        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.subscription = subscription
        self.fail_if_not_exists = fail_if_not_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        self.log.info("Deleting subscription %s", self.subscription)
        hook.delete_subscription(
            project_id=self.project_id,
            subscription=self.subscription,
            fail_if_not_exists=self.fail_if_not_exists,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata
        )
        self.log.info("Deleted subscription %s", self.subscription)


class PubSubPublishOperator(BaseOperator):
    """Publish messages to a PubSub topic.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubPublishOperator`

    Each Task publishes all provided messages to the same topic
    in a single GCP project. If the topic does not exist, this
    task will fail. ::

        m1 = {'data': b'Hello, World!',
              'attributes': {'type': 'greeting'}
             }
        m2 = {'data': b'Knock, knock'}
        m3 = {'attributes': {'foo': ''}}

        t1 = PubSubPublishOperator(
            project='my-project',topic='my_topic',
            messages=[m1, m2, m3],
            create_topic=True,
            dag=dag)

    ``project`` , ``topic``, and ``messages`` are templated so you can use
    variables in them.

    :param project_id: Optional, the GCP project ID in which to work (templated).
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param topic: the topic to which to publish. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :type topic: str
    :param messages: a list of messages to be published to the
        topic. Each message is a dict with one or more of the
        following keys-value mappings:
        * 'data': a bytestring (utf-8 encoded)
        * 'attributes': {'key1': 'value1', ...}
        Each message must contain at least a non-empty 'data' value
        or an attribute dict with at least one key (templated). See
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    :type messages: list
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request
        must have domain-wide delegation enabled.
    :type delegate_to: str
    :param project: (Deprecated) the GCP project ID where the topic will be created
    :type project: str
    """
    template_fields = ['project_id', 'topic', 'messages']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            topic: str,
            messages: List,
            project_id: Optional[str] = None,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            project: Optional[str] = None,
            *args,
            **kwargs) -> None:

        # To preserve backward compatibility
        # TODO: remove one day
        if project:
            warnings.warn(
                "The project parameter has been deprecated. You should pass "
                "the project_id parameter.", DeprecationWarning, stacklevel=2)
            project_id = project

        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.topic = topic
        self.messages = messages
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        self.log.info("Publishing to topic %s", self.topic)
        hook.publish(project_id=self.project_id, topic=self.topic, messages=self.messages)
        self.log.info("Published to topic %s", self.topic)
