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

from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PubSubTopicCreateOperator(BaseOperator):
    """Create a PubSub topic.

    By default, if the topic already exists, this operator will
    not cause the DAG to fail.
    ```
    with DAG('successful DAG') as dag:
        (
            dag
            >> PubSubTopicCreateOperator(topic='my_new_topic')
            >> PubSubTopicCreateOperator(topic='my_new_topic')
        )
    ```

    The operator can be configured to fail if the topic already exists.
    ```
    with DAG('failing DAG') as dag:
        (
            dag
            >> PubSubTopicCreateOperator(topic='my_new_topic')
            >> PubSubTopicCreateOperator(topic='my_new_topic',
                                         fail_if_exists=True)
        )
    ```

    Both ``project`` and ``topic`` are templated so you can use
    variables in them.
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
        """
        :param project: the GCP project name or ID in which to work
            (templated)
        :type project: string
        :param topic: the topic to create. Do not include the
            full topic path. In other words, instead of
            ``projects/{project}/topics/{topic}``, provide only
            ``{topic}``. (templated)
        :type topic: string
        :param gcp_conn_id: The connection ID to use connecting to
            Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request
            must have domain-wide delegation enabled.
        :type delegate_to: string
        """
        super(PubSubTopicCreateOperator, self).__init__(*args, **kwargs)

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


class PubSubPublishOperator(BaseOperator):
    """Publish messages to a PubSub topic.

    Each Task publishes all provided messages to the same topic
    in a single GCP project. If the topic does not exist, this
    task will fail.

    ```
    from base64 import b64encode as b64e

    m1 = {'data': b64e('Hello, World!'),
          'attributes': {'type': 'greeting'}
         }
    m2 = {'data': b64e('Knock, knock')}
    m3 = {'attributes': {'foo': ''}}

    t1 = PubSubPublishOperator(
        topic='my_topic',
        messages=[m1, m2, m3],
        create_topic=True,
        dag=dag)
    ```
    Both ``project`` and ``topic`` are templated so you can use
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
        :param project: the GCP project name or ID in which to work
            (templated)
        :type project: string
        :param topic: the topic to which to publish. Do not include the
            full topic path. In other words, instead of
            ``projects/{project}/topics/{topic}``, provide only
            ``{topic}``. (templated)
        :type topic: string
        :param messages: a list of messages to be published to the
            topic. Each message is a dict with one or more of the
            following keys-value mappings:
            * 'data': a base64-encoded string
            * 'attributes': {'key1': 'value1', ...}
            Each message must contain at least a non-empty 'data' value
            or an attribute dict with at least one key. See
            https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
            (templated)
        :type messages: list
        :param gcp_conn_id: The connection ID to use connecting to
            Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request
            must have domain-wide delegation enabled.
        :type delegate_to: string
        """
        super(PubSubPublishOperator, self).__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project = project
        self.topic = topic
        self.messages = messages

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)
        hook.publish(self.project, self.topic, self.messages)
