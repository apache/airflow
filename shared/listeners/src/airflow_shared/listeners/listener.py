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

from typing import TYPE_CHECKING

import pluggy
import structlog

if TYPE_CHECKING:
    from pluggy._hooks import _HookRelay

log = structlog.get_logger(__name__)


def _before_hookcall(hook_name, hook_impls, kwargs):
    log.debug("Calling %r with %r", hook_name, kwargs)
    log.debug("Hook impls: %s", hook_impls)


def _after_hookcall(outcome, hook_name, hook_impls, kwargs):
    log.debug("Result from %r: %s", hook_name, outcome.get_result())


class ListenerManager:
    """
    Manage listener registration and provides hook property for calling them.

    This class provides base infra for listener system. The consumers / components
    wanting to register listeners should initialise its own ListenerManager and
    register the hook specs relevant to that component using add_hookspecs.
    """

    def __init__(self):
        self.pm = pluggy.PluginManager("airflow")
        self.pm.add_hookcall_monitoring(_before_hookcall, _after_hookcall)

    def add_hookspecs(self, spec_module) -> None:
        """
        Register hook specs from a module.

        :param spec_module: A module containing functions decorated with @hookspec.
        """
        self.pm.add_hookspecs(spec_module)

    @property
    def has_listeners(self) -> bool:
        return bool(self.pm.get_plugins())

    @property
    def hook(self) -> _HookRelay:
        """Return hook, on which plugin methods specified in spec can be called."""
        return self.pm.hook

    def add_listener(self, listener):
        if self.pm.is_registered(listener):
            return
        self.pm.register(listener)

    def clear(self):
        """Remove registered plugins."""
        for plugin in self.pm.get_plugins():
            self.pm.unregister(plugin)
