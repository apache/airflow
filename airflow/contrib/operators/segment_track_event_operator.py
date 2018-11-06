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

from airflow.contrib.hooks.segment_hook import SegmentHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SegmentTrackEventOperator(BaseOperator):
    """
    Send Track Event to Segment for a specified user_id and event

    :param user_id: The ID for this user in your database. (templated)
    :type user_id: str
    :param event: The name of the event you're tracking. (templated)
    :type event: str
    :param properties: A dictionary of properties for the event. (templated)
    :type properties: dict
    :param segment_conn_id: The connection ID to use when connecting to Segment.
    :type segment_conn_id: str
    :param segment_debug_mode: Determines whether Segment should run in debug mode.
        Defaults to False
    :type segment_debug_mode: bool
    """
    template_fields = ('user_id', 'event', 'properties')
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 user_id,
                 event,
                 properties=None,
                 segment_conn_id='segment_default',
                 segment_debug_mode=False,
                 *args,
                 **kwargs):
        super(SegmentTrackEventOperator, self).__init__(*args, **kwargs)
        self.user_id = user_id
        self.event = event
        properties = properties or {}
        self.properties = properties
        self.segment_debug_mode = segment_debug_mode
        self.segment_conn_id = segment_conn_id

    def execute(self, context):
        hook = SegmentHook(segment_conn_id=self.segment_conn_id,
                           segment_debug_mode=self.segment_debug_mode)

        self.log.info(
            'Sending track event ({0}) for user id: {1} with properties: {2}'.
            format(self.event, self.user_id, self.properties))

        hook.track(self.user_id, self.event, self.properties)
