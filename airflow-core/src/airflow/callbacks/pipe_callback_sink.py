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
from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Callable

from airflow.callbacks.base_callback_sink import BaseCallbackSink

if TYPE_CHECKING:
    from multiprocessing.connection import Connection as MultiprocessingConnection

    from airflow.callbacks.callback_requests import CallbackRequest


class PipeCallbackSink(BaseCallbackSink):
    """
    Class for sending callbacks to DagProcessor using pipe.

    It is used when DagProcessor is not executed in standalone mode.
    """

    def __init__(self, get_sink_pipe: Callable[[], MultiprocessingConnection]):
        self._get_sink_pipe = get_sink_pipe

    def send(self, callback: CallbackRequest):
        """
        Send information about the callback to be executed by Pipe.

        :param callback: Callback request to be executed.
        """
        with contextlib.suppress(ConnectionError):
            # If this died cos of an error then we will noticed and restarted
            # when harvest_serialized_dags calls _heartbeat_manager.
            self._get_sink_pipe().send(callback)
