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

from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PubSubTopicCreateOperator(BaseOperator):
    """Create a PubSub topic.

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

    :param project: the GCP project ID where the topic will be created
    :type project: str
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
    """
    template_fields = ['project', 'topic']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            project,
            topic,
            fail_if_exists=False,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)

        self.project = project
        self.topic = topic
        self.fail_if_exists = fail_if_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        hook.create_topic(self.project, self.topic,
                          fail_if_exists=self.fail_if_exists)


class PubSubSubscriptionCreateOperator(BaseOperator):
    """Create a PubSub subscription.

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
    """
    template_fields = ['topic_project', 'topic', 'subscription',
                       'subscription_project']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            topic_project,
            topic,
            subscription=None,
            subscription_project=None,
            ack_deadline_secs=10,
            fail_if_exists=False,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        :param topic_project: the GCP project ID where the topic exists
        :type topic_project: str
        :param topic: the topic to create. Do not include the
            full topic path. In other words, instead of
            ``projects/{project}/topics/{topic}``, provide only
            ``{topic}``. (templated)
        :type topic: str
        :param subscription: the Pub/Sub subscription name. If empty, a random
            name will be generated using the uuid module
        :type subscription: str
        :param subscription_project: the GCP project ID where the subscription
            will be created. If empty, ``topic_project`` will be used.
        :type subscription_project: str
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
        """
        super().__init__(*args, **kwargs)

        self.topic_project = topic_project
        self.topic = topic
        self.subscription = subscription
        self.subscription_project = subscription_project
        self.ack_deadline_secs = ack_deadline_secs
        self.fail_if_exists = fail_if_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        return hook.create_subscription(
            self.topic_project, self.topic, self.subscription,
            self.subscription_project, self.ack_deadline_secs,
            self.fail_if_exists)


class PubSubTopicDeleteOperator(BaseOperator):
    """Delete a PubSub topic.

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
    """
    template_fields = ['project', 'topic']
    ui_color = '#cb4335'

    @apply_defaults
    def __init__(
            self,
            project,
            topic,
            fail_if_not_exists=False,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        :param project: the GCP project ID in which to work (templated)
        :type project: str
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
        """
        super().__init__(*args, **kwargs)

        self.project = project
        self.topic = topic
        self.fail_if_not_exists = fail_if_not_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        hook.delete_topic(self.project, self.topic,
                          fail_if_not_exists=self.fail_if_not_exists)


class PubSubSubscriptionDeleteOperator(BaseOperator):
    """Delete a PubSub subscription.

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
    """
    template_fields = ['project', 'subscription']
    ui_color = '#cb4335'

    @apply_defaults
    def __init__(
            self,
            project,
            subscription,
            fail_if_not_exists=False,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        :param project: the GCP project ID in which to work (templated)
        :type project: str
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
        """
        super().__init__(*args, **kwargs)

        self.project = project
        self.subscription = subscription
        self.fail_if_not_exists = fail_if_not_exists
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)

        hook.delete_subscription(self.project, self.subscription,
                                 fail_if_not_exists=self.fail_if_not_exists)


class PubSubPublishOperator(BaseOperator):
    """Publish messages to a PubSub topic.

    Each Task publishes all provided messages to the same topic
    in a single GCP project. If the topic does not exist, this
    task will fail. ::

        from base64 import b64encode as b64e

        m1 = {'data': b64e('Hello, World!'),
              'attributes': {'type': 'greeting'}
             }
        m2 = {'data': b64e('Knock, knock')}
        m3 = {'attributes': {'foo': ''}}

        t1 = PubSubPublishOperator(
            project='my-project',topic='my_topic',
            messages=[m1, m2, m3],
            create_topic=True,
            dag=dag)

    ``project`` , ``topic``, and ``messages`` are templated so you can use
    variables in them.
    """
    template_fields = ['project', 'topic', 'messages']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            project,
            topic,
            messages,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        :param project: the GCP project ID in which to work (templated)
        :type project: str
        :param topic: the topic to which to publish. Do not include the
            full topic path. In other words, instead of
            ``projects/{project}/topics/{topic}``, provide only
            ``{topic}``. (templated)
        :type topic: str
        :param messages: a list of messages to be published to the
            topic. Each message is a dict with one or more of the
            following keys-value mappings:
            * 'data': a base64-encoded string
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
        """
        super().__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project = project
        self.topic = topic
        self.messages = messages

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)
        hook.publish(self.project, self.topic, self.messages)
