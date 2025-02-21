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

#
# Centralized handling of the connection mechanism for all Message Queues 
# 

from collections.abc import Sequence
from typing import Any, NoReturn

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowOptionalProviderFeatureException,
)
from airflow.hooks.base import BaseHook


class MsgQueueHook(BaseHook): 
    """
    Abstract base class for all Message Queue Hooks. 
    """

    # Typical parameters below

    # Override to provide connection name
    conn_name_attr: str
    # Override to have a default connection id for a particular msg queue
    default_conn_name = "default_conn_id"
    # Connection type - for types of message queues
    conn_type = "kafka"
    # Hook name
    hook_name = "Apache Kafka"

    def __init__(self, *args, **kwargs):
        super().__init__()
        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        elif len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])

    def get_conn_id(self) -> str:
        return getattr(self, self.conn_name_attr)


    def get_conn(self) -> Any:
        """Return a connection object."""
        queue = self.connection
        if self.connector is None:
            raise RuntimeError(f"{type(self).__name__} didn't have `self.connector` set!")
        return self.connector.connect(host=queue.host, port=queue.port, username=queue.login)


class MsgQueueConsumerHook(MsgQueueHook):
    """
    Abstract base class hook for creating a message queue consumer. 

    :param connection configuration information, default to BaseHook configuration
    :param topics: A list of topics to subscribe to on the message queue
    """

    def __init__(self, topics: Sequence[str], config_id=MsgQueueHook.default_conn_name) -> None:
        super().__init__(config_id=config_id)
        self.topics = topics


