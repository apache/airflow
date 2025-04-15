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

from typing import Any


def format_tags(source: Any, *, key_label: str = "Key", value_label: str = "Value"):
    """
    Format tags for boto call which expect a given format.

    If given a dictionary, formats it as an array of objects with a key and a value field to be passed to boto
    calls that expect this format.

    Else, assumes that it's already in the right format and returns it as is. We do not validate
    the format here since it's done by boto anyway, and the error would not be clearer if thrown from here.

    :param source: a dict from which keys and values are read
    :param key_label: optional, the label to use for keys if not "Key"
    :param value_label: optional, the label to use for values if not "Value"
    """
    if source is None:
        return []
    if isinstance(source, dict):
        return [{key_label: kvp[0], value_label: kvp[1]} for kvp in source.items()]
    return source
