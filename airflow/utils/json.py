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
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from flask.json.provider import JSONProvider

from airflow.serialization.serde import CLASSNAME, DATA, SCHEMA_ID, deserialize, serialize
from airflow.utils.timezone import convert_to_utc, is_naive


class AirflowJsonProvider(JSONProvider):
    """JSON Provider for Flask app to use WebEncoder."""

    ensure_ascii: bool = True
    sort_keys: bool = True

    def dumps(self, obj, **kwargs):
        kwargs.setdefault("ensure_ascii", self.ensure_ascii)
        kwargs.setdefault("sort_keys", self.sort_keys)
        return json.dumps(obj, **kwargs, cls=WebEncoder)

    def loads(self, s: str | bytes, **kwargs):
        return json.loads(s, **kwargs)


class WebEncoder(json.JSONEncoder):
    """This encodes values into a web understandable format. There is no deserializer"""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            if is_naive(o):
                o = convert_to_utc(o)
            return o.isoformat()

        if isinstance(o, date):
            return o.strftime("%Y-%m-%d")

        if isinstance(o, Decimal):
            data = serialize(o)
            if isinstance(data, dict) and DATA in data:
                return data[DATA]

        try:
            data = serialize(o)
            if isinstance(data, dict) and CLASSNAME in data:
                # this is here for backwards compatibility
                if (
                    data[CLASSNAME].startswith("numpy")
                    or data[CLASSNAME] == "kubernetes.client.models.v1_pod.V1Pod"
                ):
                    return data[DATA]
            return data
        except TypeError:
            raise


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

        return super().encode(o)


class XComDecoder(json.JSONDecoder):
    """
    This decoder deserializes dicts to objects if they contain
    the `__classname__` key otherwise it will return the dict
    as is.
    """

    def __init__(self, *args, **kwargs) -> None:
        if not kwargs.get("object_hook"):
            kwargs["object_hook"] = self.object_hook

        super().__init__(*args, **kwargs)

    def object_hook(self, dct: dict) -> object:
        return deserialize(dct)

    @staticmethod
    def orm_object_hook(dct: dict) -> object:
        """Creates a readable representation of a serialized object"""
        return deserialize(dct, False)


# backwards compatibility
AirflowJsonEncoder = WebEncoder
