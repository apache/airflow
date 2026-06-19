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
"""Validation helper for the ``ui_color`` / ``ui_fgcolor`` graph node colors."""

from __future__ import annotations

from typing import TypeGuard


def is_chakra_color_token(value: str | None) -> TypeGuard[str]:
    """
    Return whether ``value`` is shaped like a Chakra UI color token (``family.shade``).

    A token is ``<family>.<shade>`` such as ``blue.500`` -- a color family (built-in or one added
    through custom UI theming) followed by an integer shade. Only the shape is checked, with plain
    string operations rather than a regular expression so a Dag author cannot craft a value that
    triggers pathological backtracking. Unknown families simply resolve to an undefined CSS variable
    and are ignored by the UI. Raw hex (``#fff``), ``rgb()``/``hsl()`` and bare CSS names
    (``CornflowerBlue``) are rejected.
    """
    if not value or not value.isascii():
        return False
    family, separator, shade = value.partition(".")
    if not separator:
        return False
    return family[:1].isalpha() and family.isalnum() and shade.isdigit()
