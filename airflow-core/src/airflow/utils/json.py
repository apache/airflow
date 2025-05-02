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

import json
from typing import Any

from airflow.serialization.serde import CLASSNAME, SCHEMA_ID, deserialize, serialize


class XComEncoder(json.JSONEncoder):
    """This encoder serializes any object that has attr, dataclass or a custom serializer."""

    def default(self, o: object) -> Any:
        try:
            return serialize(o)
        except TypeError:
            return super().default(o)

    def encode(self, o: Any) -> str:
        # checked here and in serialize
        if isinstance(o, dict) and (CLASSNAME in o or SCHEMA_ID in o):
            raise AttributeError(f"reserved key {CLASSNAME} found in dict to serialize")

        # tuples are not preserved by std python serializer
        if isinstance(o, tuple):
            o = self.default(o)

        return super().encode(o)


class XComDecoder(json.JSONDecoder):
    """Deserialize dicts to objects if they contain the `__classname__` key, otherwise return the dict."""

    def __init__(self, *args, **kwargs) -> None:
        if not kwargs.get("object_hook"):
            kwargs["object_hook"] = self.object_hook

        super().__init__(*args, **kwargs)

    def object_hook(self, dct: dict) -> object:
        return deserialize(dct)

    @staticmethod
    def orm_object_hook(dct: dict) -> object:
        """Create a readable representation of a serialized object."""
        return deserialize(dct, False)
