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

import sys


class _autostacklevel_warn:
    def __init__(self, delta):
        self.warnings = __import__("warnings")
        self.delta = delta

    def __getattr__(self, name):
        return getattr(self.warnings, name)

    def __dir__(self):
        return dir(self.warnings)

    def warn(self, message, category=None, stacklevel=1, source=None):
        self.warnings.warn(message, category, stacklevel + self.delta, source)


def fixup_decorator_warning_stack(func, delta: int = 2):
    if func.__globals__.get("warnings") is sys.modules["warnings"]:
        # Yes, this is more than slightly hacky, but it _automatically_ sets the right stacklevel parameter to
        # `warnings.warn` to ignore the decorator.
        func.__globals__["warnings"] = _autostacklevel_warn(delta)
