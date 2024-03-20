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
Connect to your Segment account, retrieve data from it or write to that file.

NOTE:   this hook also relies on the Segment analytics package:
        https://github.com/segmentio/analytics-python
"""

from __future__ import annotations

import analytics

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class SegmentHook(BaseHook):
    """
    Create new connection to Segment and allows you to pull data out of Segment or write to it.

    You can then use that file with other
    Airflow operators to move the data around or interact with segment.

    :param segment_conn_id: the name of the connection that has the parameters
        we need to connect to Segment. The connection should be type `json` and include a
        write_key security token in the `Extras` field.
    :param segment_debug_mode: Determines whether Segment should run in debug mode.
        Defaults to False

    .. note::
        You must include a JSON structure in the `Extras` field.
        We need a user's security token to connect to Segment.
        So we define it in the `Extras` field as:
        `{"write_key":"YOUR_SECURITY_TOKEN"}`
    """

    conn_name_attr = "segment_conn_id"
    default_conn_name = "segment_default"
    conn_type = "segment"
    hook_name = "Segment"

    def __init__(
        self, segment_conn_id: str = "segment_default", segment_debug_mode: bool = False, *args, **kwargs
    ) -> None:
        super().__init__()
        self.segment_conn_id = segment_conn_id
        self.segment_debug_mode = segment_debug_mode
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(self.segment_conn_id)
        self.extras = self.connection.extra_dejson
        self.write_key = self.extras.get("write_key")
        if self.write_key is None:
            raise AirflowException("No Segment write key provided")

    def get_conn(self) -> analytics:
        self.log.info("Setting write key for Segment analytics connection")
        analytics.debug = self.segment_debug_mode
        if self.segment_debug_mode:
            self.log.info("Setting Segment analytics connection to debug mode")
        analytics.on_error = self.on_error
        analytics.write_key = self.write_key
        return analytics

    def on_error(self, error: str, items: str) -> None:
        """Handle error callbacks when using Segment with segment_debug_mode set to True."""
        self.log.error("Encountered Segment error: %s with items: %s", error, items)
        raise AirflowException(f"Segment error: {error}")
