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

from airflow.notifiers import AirflowNotifier
from airflow.utils.email import send_email
from airflow.utils.state import State


class EmailNotifier(AirflowNotifier):

    def __init__(self, email, notify_on=None):
        self.email = email

        if notify_on is None:
            self.notify_on = {State.UP_FOR_RETRY: ['text_title.tpl', 'html_body.tpl'],
                              State.FAILED: ['text_title.tpl', 'html_body.tpl']}
        else:
            self.notify_on = notify_on

    def send_notification(self, task_instance, state, message, **kwargs):
        if state in self.notify_on:
            title_tpl, body_tpl = self.notify_on[state]

            title = self.render_template(title_tpl,
                                         task_instance,
                                         message=message)
            body = self.render_template(body_tpl,
                                        task_instance,
                                        message=message,
                                        **kwargs)

            send_email(self.email, title, body)
