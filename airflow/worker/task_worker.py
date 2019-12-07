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
This module contains task task workers.
"""
import setproctitle
from kombu import Connection

from airflow import conf
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.worker.consumer import TaskConsumer
from airflow.worker.utils import add_signal_handler, unique_suffix


class TaskWorker(LoggingMixin):
    """
    Single instance of task worker that runs a single TaskConsumer.
    The worker kills gracefully on SIGTERM and SIGINT.
    """

    def __init__(self, name: str = "airflow-tw") -> None:
        self.name = unique_suffix(name)
        self.amq_url = conf.get("worker", "broker_url")
        self.queues = conf.get("worker", "queues").split(",")
        with Connection(self.amq_url) as conn:
            self.consumer = TaskConsumer(conn, queues=self.queues, name=self.name)

    def start(self) -> None:
        """
        Starts TaskConsumer.
        """
        # Add signal handler for TaskWorker, done here because it's a
        # starting point for new process
        add_signal_handler(self.stop)

        setproctitle.setproctitle(self.name)
        self.log.debug("Consuming task from %s on %s", self.queues, self.amq_url)
        self.consumer.run()

    def stop(self, signum, frame) -> None:  # pylint: disable=unused-argument
        """
        Stops TaskConsumer.
        """
        self.log.debug("Stopping task worker: %s", self.name)
        self.consumer.stop()
