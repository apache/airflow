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

from uuid import uuid4

from apiclient.discovery import build
from apiclient import errors

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


def _format_subscription(project, subscription):
    return 'projects/{}/subscriptions/{}'.format(project, subscription)


def _format_topic(project, topic):
    return 'projects/{}/topics/{}'.format(project, topic)


class PubSubException(Exception):
    pass


class PubSubHook(GoogleCloudBaseHook):
    """Hook for accessing Google Pub/Sub.

    The GCP project against which actions are applied is determined by
    the project embedded in the Connection referenced by gcp_conn_id.
    """

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        super(PubSubHook, self).__init__(gcp_conn_id, delegate_to=delegate_to)

    def get_conn(self):
        """Returns a Pub/Sub service object.

        :rtype: apiclient.discovery.Resource
        """
        http_authorized = self._authorize()
        return build(
            'pubsub', 'v1', http=http_authorized, cache_discovery=False)

    def publish(self, project, topic, messages):
        """Publishes messages to a Pub/Sub topic.

        :param project: the GCP project ID in which to publish
        :type project: str
        :param topic: the Pub/Sub topic to which to publish; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param messages: messages to publish; if the data field in a
            message is set, it should already be base64 encoded.
        :type messages: list of PubSub messages; see
            http://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        """
        body = {'messages': messages}
        full_topic = _format_topic(project, topic)
        request = self.get_conn().projects().topics().publish(
            topic=full_topic, body=body)
        try:
            request.execute()
        except errors.HttpError as e:
            raise PubSubException(
                'Error publishing to topic {}'.format(full_topic), e)

    def create_topic(self, project, topic, fail_if_exists=False):
        """Creates a Pub/Sub topic, if it does not already exist.

        :param project: the GCP project ID in which to create
            the topic
        :type project: str
        :param topic: the Pub/Sub topic name to create; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param fail_if_exists: if set, raise an exception if the topic
            already exists
        :type fail_if_exists: bool
        """
        service = self.get_conn()
        full_topic = _format_topic(project, topic)
        try:
            service.projects().topics().create(
                name=full_topic, body={}).execute()
        except errors.HttpError as e:
            # Status code 409 indicates that the topic already exists.
            if str(e.resp['status']) == '409':
                message = 'Topic already exists: {}'.format(full_topic)
                self.log.warning(message)
                if fail_if_exists:
                    raise PubSubException(message)
            else:
                raise PubSubException(
                    'Error creating topic {}'.format(full_topic), e)

    def delete_topic(self, project, topic, fail_if_not_exists=False):
        """Deletes a Pub/Sub topic if it exists.

        :param project: the GCP project ID in which to delete the topic
        :type project: str
        :param topic: the Pub/Sub topic name to delete; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param fail_if_not_exists: if set, raise an exception if the topic
            does not exist
        :type fail_if_not_exists: bool
        """
        service = self.get_conn()
        full_topic = _format_topic(project, topic)
        try:
            service.projects().topics().delete(topic=full_topic).execute()
        except errors.HttpError as e:
            # Status code 409 indicates that the topic was not found
            if str(e.resp['status']) == '404':
                message = 'Topic does not exist: {}'.format(full_topic)
                self.log.warning(message)
                if fail_if_not_exists:
                    raise PubSubException(message)
            else:
                raise PubSubException(
                    'Error deleting topic {}'.format(full_topic), e)

    def create_subscription(self, topic_project, topic, subscription=None,
                            subscription_project=None, ack_deadline_secs=10,
                            fail_if_exists=False):
        """Creates a Pub/Sub subscription, if it does not already exist.

        :param topic_project: the GCP project ID of the topic that the
            subscription will be bound to.
        :type topic_project: str
        :param topic: the Pub/Sub topic name that the subscription will be bound
            to create; do not include the ``projects/{project}/subscriptions/``
            prefix.
        :type topic: str
        :param subscription: the Pub/Sub subscription name. If empty, a random
            name will be generated using the uuid module
        :type subscription: str
        :param subscription_project: the GCP project ID where the subscription
            will be created. If unspecified, ``topic_project`` will be used.
        :type subscription_project: str
        :param ack_deadline_secs: Number of seconds that a subscriber has to
            acknowledge each message pulled from the subscription
        :type ack_deadline_secs: int
        :param fail_if_exists: if set, raise an exception if the topic
            already exists
        :type fail_if_exists: bool
        :return: subscription name which will be the system-generated value if
            the ``subscription`` parameter is not supplied
        :rtype: str
        """
        service = self.get_conn()
        full_topic = _format_topic(topic_project, topic)
        if not subscription:
            subscription = 'sub-{}'.format(uuid4())
        if not subscription_project:
            subscription_project = topic_project
        full_subscription = _format_subscription(subscription_project,
                                                 subscription)
        body = {
            'topic': full_topic,
            'ackDeadlineSeconds': ack_deadline_secs
        }
        try:
            service.projects().subscriptions().create(
                name=full_subscription, body=body).execute()
        except errors.HttpError as e:
            # Status code 409 indicates that the subscription already exists.
            if str(e.resp['status']) == '409':
                message = 'Subscription already exists: {}'.format(
                    full_subscription)
                self.log.warning(message)
                if fail_if_exists:
                    raise PubSubException(message)
            else:
                raise PubSubException(
                    'Error creating subscription {}'.format(full_subscription),
                    e)
        return subscription

    def delete_subscription(self, project, subscription,
                            fail_if_not_exists=False):
        """Deletes a Pub/Sub subscription, if it exists.

        :param project: the GCP project ID where the subscription exists
        :type project: str
        :param subscription: the Pub/Sub subscription name to delete; do not
            include the ``projects/{project}/subscriptions/`` prefix.
        :type subscription: str
        :param fail_if_not_exists: if set, raise an exception if the topic
            does not exist
        :type fail_if_not_exists: bool
        """
        service = self.get_conn()
        full_subscription = _format_subscription(project, subscription)
        try:
            service.projects().subscriptions().delete(
                subscription=full_subscription).execute()
        except errors.HttpError as e:
            # Status code 404 indicates that the subscription was not found
            if str(e.resp['status']) == '404':
                message = 'Subscription does not exist: {}'.format(
                    full_subscription)
                self.log.warning(message)
                if fail_if_not_exists:
                    raise PubSubException(message)
            else:
                raise PubSubException(
                    'Error deleting subscription {}'.format(full_subscription),
                    e)

    def pull(self, project, subscription, max_messages,
             return_immediately=False):
        """Pulls up to ``max_messages`` messages from Pub/Sub subscription.

        :param project: the GCP project ID where the subscription exists
        :type project: str
        :param subscription: the Pub/Sub subscription name to pull from; do not
            include the 'projects/{project}/topics/' prefix.
        :type subscription: str
        :param max_messages: The maximum number of messages to return from
            the Pub/Sub API.
        :type max_messages: int
        :param return_immediately: If set, the Pub/Sub API will immediately
            return if no messages are available. Otherwise, the request will
            block for an undisclosed, but bounded period of time
        :type return_immediately: bool
        :return A list of Pub/Sub ReceivedMessage objects each containing
            an ``ackId`` property and a ``message`` property, which includes
            the base64-encoded message content. See
            https://cloud.google.com/pubsub/docs/reference/rest/v1/\
                projects.subscriptions/pull#ReceivedMessage
        """
        service = self.get_conn()
        full_subscription = _format_subscription(project, subscription)
        body = {
            'maxMessages': max_messages,
            'returnImmediately': return_immediately
        }
        try:
            response = service.projects().subscriptions().pull(
                subscription=full_subscription, body=body).execute()
            return response.get('receivedMessages', [])
        except errors.HttpError as e:
            raise PubSubException(
                'Error pulling messages from subscription {}'.format(
                    full_subscription), e)

    def acknowledge(self, project, subscription, ack_ids):
        """Pulls up to ``max_messages`` messages from Pub/Sub subscription.

        :param project: the GCP project name or ID in which to create
            the topic
        :type project: str
        :param subscription: the Pub/Sub subscription name to delete; do not
            include the 'projects/{project}/topics/' prefix.
        :type subscription: str
        :param ack_ids: List of ReceivedMessage ackIds from a previous pull
            response
        :type ack_ids: list
        """
        service = self.get_conn()
        full_subscription = _format_subscription(project, subscription)
        try:
            service.projects().subscriptions().acknowledge(
                subscription=full_subscription, body={'ackIds': ack_ids}
            ).execute()
        except errors.HttpError as e:
            raise PubSubException(
                'Error acknowledging {} messages pulled from subscription {}'
                .format(len(ack_ids), full_subscription), e)
