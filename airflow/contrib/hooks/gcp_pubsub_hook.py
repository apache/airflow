# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from apiclient.discovery import build
from apiclient import errors

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


def _format_topic(project, topic):
    return 'projects/%s/topics/%s' % (project, topic)


class PubSubHook(GoogleCloudBaseHook):
    """Hook for accessing Google Pub/Sub.

    The GCP project against which actions are applied is determined by
    the project embedded in the Connection referenced by gcp_conn_id.
    """

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(PubSubHook, self).__init__(gcp_conn_id, delegate_to=delegate_to)

    def get_conn(self):
        """Returns a Pub/Sub service object.

        :rtype: apiclient.discovery.Resource
        """
        http_authorized = self._authorize()
        return build('pubsub', 'v1', http=http_authorized)

    def publish(self, project, topic, messages):
        """Publishes messages to a Pub/Sub topic.

        :param project: the GCP project name or ID in which to publish
        :type project: string
        :param topic: the Pub/Sub topic to which to publish; do not
            include the 'projects/{project}/topics/' prefix.
        :type topic: string
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
            raise Exception('Error publishing to topic %s' % full_topic, e)

    def create_topic(self, project, topic, fail_if_exists=False):
        """Creates a Pub/Sub topic, if it does not already exist.

        :param project: the GCP project name or ID in which to create
            the topic
        :type project: string
        :param topic: the Pub/Sub topic name to create; do not
            include the 'projects/{project}/topics/' prefix.
        :type topic: string
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
                if fail_if_exists:
                    raise Exception(
                        'Error creating topic. Topic already exists: %s'
                        % full_topic)
            else:
                raise Exception('Error creating topic %s' % full_topic, e)
