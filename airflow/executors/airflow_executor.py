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
"""Airflow executor."""

from typing import Any, Optional

from kombu import Connection, Queue

from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKeyType
from airflow.worker.exchange import task_exchange


class AirflowExecutor(BaseExecutor):
    """
    Airflow-native executor. It uses an AMQ as a broker for publishing
    tasks that are consumed by ``airflow.worker.Worker``.

    It uses Kombu for messaging.
    """

    def __init__(self):
        super().__init__()

        self.amq_url: str = conf.get("worker", "broker_url")
        with Connection(self.amq_url) as conn:
            self.producer = conn.Producer(serializer="json")

    def start(self) -> None:
        self.log.debug("Starting Airflow executor with %s queue", self.amq_url)

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Overwrite trigger_tasks function from BaseExecutor
        """
        # Order tasks by priority
        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True,
        )

        # Publish tasks
        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue_name, _) = sorted_queue.pop(0)
            self.execute_async(key, command, queue_name)
            self.log.debug("Sent all tasks.")

    def sync(self) -> None:
        """Nothing to sync in Airflow executor."""

    def end(self) -> None:
        pass

    def execute_async(
        self,
        key: TaskInstanceKeyType,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ):
        """
        Publishes task to a AMQ queue.
        """
        kombu_queue = Queue(queue, task_exchange, routing_key=queue)
        self.producer.publish(
            {"cmd": command},
            exchange=task_exchange,
            routing_key=kombu_queue.routing_key,
            declare=[kombu_queue],
            retry=True,
            retry_policy={
                "interval_start": 0,  # First retry immediately,
                "interval_step": 2,  # then increase by 2s for every retry.
                "interval_max": 3,  # but don't exceed 3s between retries.
                "max_retries": 1,  # give up after 1 tries.
            },
            expiration=2 * 60,
        )
        self.queued_tasks.pop(key)

    def terminate(self):
        pass
