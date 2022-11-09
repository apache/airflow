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

import re
import sys
from typing import Match

# We need to explicitly clear the warning registry context
# https://docs.python.org/2/library/warnings.html
# One thing to be aware of is that if a warning has already been raised because
# of a once/default rule, then no matter what filters are set the warning will
# not be seen again unless the warnings registry related to the warning has
# been cleared.
#


# Proposed fix from Stack overflow, which refers to the Python bug-page
# https://stackoverflow.com/questions/19428761/python-showing-once-warnings-again-resetting-all-warning-registries
class reset_warning_registry:
    """
    context manager which archives & clears warning registry for duration of
    context.

    :param pattern:
          optional regex pattern, causes manager to only reset modules whose
          names match this pattern. defaults to ``".*"``.
    """

    #: regexp for filtering which modules are reset
    _pattern: Match[str] | None = None

    #: dict mapping module name -> old registry contents
    _backup: dict | None = None

    def __init__(self, pattern=None):
        self._pattern = re.compile(pattern or ".*")

    def __enter__(self):
        # archive and clear the __warningregistry__ key for all modules
        # that match the 'reset' pattern.
        pattern = self._pattern
        backup = self._backup = {}
        for name, mod in list(sys.modules.items()):
            if pattern.match(name):
                reg = getattr(mod, "__warningregistry__", None)
                if reg and isinstance(reg, dict):
                    backup[name] = reg.copy()
                    reg.clear()
        return self

    def __exit__(self, *exc_info):
        # restore warning registry from backup
        modules = sys.modules
        backup = self._backup
        for name, content in backup.items():
            mod = modules.get(name)
            if mod is None:
                continue
            reg = getattr(mod, "__warningregistry__", None)
            if reg is None:
                setattr(mod, "__warningregistry__", content)
            else:
                reg.clear()
                reg.update(content)

        # clear all registry entries that we didn't archive
        pattern = self._pattern
        for name, mod in list(modules.items()):
            if pattern.match(name) and name not in backup:
                reg = getattr(mod, "__warningregistry__", None)
                if reg and isinstance(reg, dict):
                    reg.clear()
