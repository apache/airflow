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

from airflow.contrib.utils.slack_webhook import get_webhook_urls, post_message_via_webhook
from airflow.exceptions import AirflowException

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SlackWebhookOperator(BaseOperator):
    """
    You can use this operator to sent a custom message to multiple channel or DM at same time.

    Compare to old/builtin SlackOperator(), the Cons are:
    - This implementation use webhook API instead of legacy API.
    - You don't need to hard code your API token.
    - The destination where the message will be sent to is managed by webhook, more secure.
    - Webhook URLs are managed by Connections, more easier to reuse.
    - Allow you to sent a message to multiple channels or DMs in 1 operator which old one can't,
      this allow you manage the purposes and receivers.

    What you need to do:
    1. Create incoming webhook integrations to channels or users you want to send message to.
    2. Create Airflow Connection via WebUI/Admin/Connections,
       put those webhook URLs on `host` field with COMMA separated.
    3. Init an instance with specifying the conn_id you created on step 1.
    4. You're done.

    For custom/beautify your message, refer to https://api.slack.com/docs/message-attachments
    """


    @apply_defaults
    def __init__(self, slack_conn_id='slack_webhook_default',
                 text="Airflow sent this message since no message has been set.",
                 attachments=None,
                 *args, **kwargs):
        super(SlackWebhookOperator, self).__init__(*args, **kwargs)
        self.slack_conn_id = slack_conn_id
        self.text = text
        self.attachments = attachments


    def execute(self, context):
        webhook_urls = get_webhook_urls(self.slack_conn_id)
        message_json = {
            'text': self.text,
            'attachments': self.attachments
        }
        res = post_message_via_webhook(message_json, webhook_urls=webhook_urls)
        if res:
            raise AirflowException(
                "Failed to post message through Slack webhook: {}".format(res.text))
