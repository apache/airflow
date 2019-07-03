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

from airflow.contrib.hooks.dingding_hook import DingdingHook
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults


class DingdingOperator(BaseOperator):
    """
    This operator allows you send Dingding message using Dingding custom bot.
    Get Dingding token from conn_id.password. And prefer set domain to
    conn_id.host, if not will use default ``https://oapi.dingtalk.com``.

    For more detail message in
    `Dingding custom bot <https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq>`_

    :param dingding_conn_id: The name of the Dingding connection to use
    :type dingding_conn_id: str
    :param message_type: Message type you want to send to Dingding, support five type so far
        including text, link, markdown, actionCard, feedCard
    :type message_type: str
    :param message: The message send to Dingding chat group
    :type message: str or dict
    :param at_mobiles: Remind specific users with this message
    :type at_mobiles: list[str]
    :param at_all: Remind all people in group or not. If True, will overwrite ``at_mobiles``
    :type at_all: bool
    """
    template_fields = ('message',)
    ui_color = '#4ea4d4'  # Dingding icon color

    @apply_defaults
    def __init__(self,
                 dingding_conn_id='dingding_default',
                 message_type='text',
                 message=None,
                 at_mobiles=None,
                 at_all=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.dingding_conn_id = dingding_conn_id
        self.message_type = message_type
        self.message = message
        self.at_mobiles = at_mobiles
        self.at_all = at_all

    def execute(self, context):
        self.log.info('Sending Dingding message.')
        hook = DingdingHook(
            self.dingding_conn_id,
            self.message_type,
            self.message,
            self.at_mobiles,
            self.at_all
        )
        hook.send()
