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
import inspect
import logging
from typing import TYPE_CHECKING, Set

import pluggy

if TYPE_CHECKING:
    from pluggy._hooks import _HookRelay

from airflow.listeners import spec

log = logging.getLogger(__name__)


class Listener:
    """Class used as a namespace for listener hook implementation namespace"""


_listener_manager = None


class ListenerManager:
    """Class that manages registration of listeners and provides hook property for calling them"""

    def __init__(self):
        self.pm = pluggy.PluginManager("airflow")
        self.pm.add_hookspecs(spec)
        self.listener_names: Set[str] = set()

    def has_listeners(self) -> bool:
        return len(self.pm.get_plugins()) > 0

    @property
    def hook(self) -> "_HookRelay":
        """Returns hook, on which plugin methods specified in spec can be called."""
        return self.pm.hook

    def add_listener(self, listener: Listener):
        if listener.__class__.__name__ in self.listener_names:
            return
        if self.pm.is_registered(listener):
            return

        listener_type = type(listener)
        if not (
            inspect.isclass(listener_type)
            and issubclass(listener_type, Listener)
            and (listener_type is not Listener)
        ):
            log.warning("Can't register listener: %s - is not a Listener subclass", listener_type)
            return

        self.listener_names.add(listener.__class__.__name__)
        self.pm.register(listener)

    def clear(self):
        """Remove registered plugins"""
        self.listener_names = set()
        for plugin in self.pm.get_plugins():
            self.pm.unregister(plugin)


def get_listener_manager() -> ListenerManager:
    global _listener_manager
    if not _listener_manager:
        _listener_manager = ListenerManager()
    return _listener_manager
