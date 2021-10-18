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

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from airflow.models.connection import Connection

from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any, Optional

from airflow.utils.module_loading import import_string


class AbstractProcessStateBackend(ABC):
    @abstractmethod
    def get_state(
        self,
        process_name: str,
        namespace: str = 'default',
    ) -> Optional[Any]:
        """Retrieve state value from backend, given ``namespace`` and ``process_name``"""

    @abstractmethod
    def set_state(self, process_name: str, value: Any, namespace: str = 'default'):
        """Store state value ``value`` in backend for given ``namespace`` and ``process_name``."""

    @abstractmethod
    def delete(self, process_name: str, namespace: str = 'default'):
        """Delete the record for given ``namespace`` and ``process_name``."""


conf = dict(
    backend='airflow.process_state.ssm.AwsSsmProcessStateBackend',
    backend_kwargs=dict(
        prefix=f'/airflow/process_state/production',
        profile_name=None,
    ),
)

process_state_backend: Optional[AbstractProcessStateBackend] = None


class ProcessState:
    """
    For persisting state of arbitrary processes.

    Args:
        process_name: name to identify this process (must be unique within namespace)
        namespace: use for separation of a families of watermark processes from one another.
        default_value: what to return from `get_state` if no value is set.
    """

    def __init__(self, process_name, namespace='default', default_value=None, **kwargs):
        if not (namespace.islower() and process_name.islower()):
            raise ValueError('`namespace` and `process_name` must be lowercase.')
        self.namespace = namespace
        self.process_name = process_name
        self.default_value = default_value
        self.kwargs = kwargs

    @cached_property
    def backend(self):
        return process_state_backend

    def get_value(self):
        return (
            self.backend.get_state(namespace=self.namespace, process_name=self.process_name)
            or self.default_value
        )

    def set_value(self, value):
        return self.backend.set_state(
            namespace=self.namespace,
            process_name=self.process_name,
            value=value,
        )

    def delete(self):
        return self.backend.delete(namespace=self.namespace, process_name=self.process_name)


def initialize():
    global process_state_backend
    backend_name = conf.get('backend')
    backend_kwargs = conf.get('backend_kwargs')
    process_state_backend = import_string(backend_name)(**backend_kwargs)


initialize()
