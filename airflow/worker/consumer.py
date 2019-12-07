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
This module contains task consumer.
"""
import os
import subprocess
from typing import Any, Dict, List

from kombu import Connection, Consumer, Message, Queue
from kombu.mixins import ConsumerMixin

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.worker.exchange import task_exchange


class TaskConsumer(ConsumerMixin, LoggingMixin):
    """
    Task consumer.
    """

    def __init__(
        self, connection: Connection, queues: List[str], name: str = "consumer"
    ) -> None:
        self.name = name
        self.connection = connection
        self.queues = [Queue(q, task_exchange, routing_key=q) for q in queues]

    def get_consumers(self, consumer, channel) -> List[Consumer]:
        return [
            # (prefetch_count=1) That way each consumer will just grab 1 message,
            # and leave the rest in the queue with the state ready
            consumer(q, callbacks=[self.run_task], accept=["json"], prefetch_count=1)
            for q in self.queues
        ]

    @staticmethod
    def run_task(body: Dict[Any, Any], message: Message) -> None:
        """
        Executes ``airflow tasks run`` command.

        :param body: Body of the received message
        :param message: Message instance
        :return:
        """
        if "cmd" in body:
            env = os.environ.copy()
            try:
                subprocess.check_call(
                    body["cmd"], stderr=subprocess.STDOUT, close_fds=True, env=env
                )
            except subprocess.CalledProcessError:
                pass
        message.ack()

    def stop(self) -> None:
        """
        When this is set to true the consumer should stop consuming
        and return, so that it can be joined if it is the implementation
        of a thread.
        """
        self.should_stop = True
