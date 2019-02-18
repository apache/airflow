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

from airflow.contrib.hooks.imap_hook import ImapHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class ImapAttachmentSensor(BaseSensorOperator):
    """
    Waits for a specific attachment on a mail server.

    :param attachment_name: The name of the attachment that will be checked.
    :type attachment_name: str
    :param check_regex: If set to True the attachment's name will be parsed as regular expression.
                        Through this you can get a broader set of attachments
                        that it will look for than just only the equality of the attachment name.
                        The default value is False.
    :type check_regex: bool
    :param mail_folder: The mail folder in where to search for the attachment.
                        The default value is 'INBOX'.
    :type mail_folder: str
    :param conn_id: The connection to run the sensor against.
                    The default value is 'imap_default'.
    :type conn_id: str
    """
    template_fields = ('attachment_name',)

    @apply_defaults
    def __init__(self,
                 attachment_name,
                 mail_folder='INBOX',
                 check_regex=False,
                 conn_id='imap_default',
                 *args,
                 **kwargs):
        super(ImapAttachmentSensor, self).__init__(*args, **kwargs)

        self.attachment_name = attachment_name
        self.mail_folder = mail_folder
        self.check_regex = check_regex
        self.conn_id = conn_id

    def poke(self, context):
        """
        Pokes for a mail attachment on the mail server.

        :param context: The context that is being provided when poking.
        :type context: dict
        :return: True if attachment with the given name is present and False if not.
        :rtype: bool
        """
        self.log.info('Poking for %s', self.attachment_name)

        with ImapHook(imap_conn_id=self.conn_id) as imap_hook:
            return imap_hook.has_mail_attachment(
                name=self.attachment_name,
                mail_folder=self.mail_folder,
                check_regex=self.check_regex
            )
