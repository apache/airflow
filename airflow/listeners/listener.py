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
import logging
from types import ModuleType
from typing import TYPE_CHECKING

import pluggy

from airflow.plugins_manager import integrate_listener_plugins

if TYPE_CHECKING:
    from pluggy._hooks import _HookRelay

log = logging.getLogger(__name__)


_listener_manager = None


class ListenerManager:
    """Class that manages registration of listeners and provides hook property for calling them"""

    def __init__(self):
        from airflow.listeners import spec

        self.pm = pluggy.PluginManager("airflow")
        self.pm.add_hookspecs(spec)

    @property
    def has_listeners(self) -> bool:
        return len(self.pm.get_plugins()) > 0

    @property
    def hook(self) -> "_HookRelay":
        """Returns hook, on which plugin methods specified in spec can be called."""
        return self.pm.hook

    def add_listener(self, listener):
        if not isinstance(listener, ModuleType):
            raise TypeError("Listener %s is not module", str(listener))
        if self.pm.is_registered(listener):
            return
        self.pm.register(listener)

    def clear(self):
        """Remove registered plugins"""
        for plugin in self.pm.get_plugins():
            self.pm.unregister(plugin)


def get_listener_manager() -> ListenerManager:
    global _listener_manager
    if not _listener_manager:
        _listener_manager = ListenerManager()
        integrate_listener_plugins(_listener_manager)
    return _listener_manager
