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

import dataclasses
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import attr
from flask.json.provider import JSONProvider

from airflow.serialization.enums import Encoding
from airflow.utils.module_loading import import_string
from airflow.utils.timezone import convert_to_utc, is_naive

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore

try:
    from kubernetes.client import models as k8s
except ImportError:
    k8s = None

# Dates and JSON encoding/decoding

log = logging.getLogger(__name__)

CLASSNAME = "__classname__"
VERSION = "__version__"
DATA = "__data__"

OLD_TYPE = "__type"
OLD_SOURCE = "__source"
OLD_DATA = "__var"

DEFAULT_VERSION = 0


class AirflowJsonEncoder(json.JSONEncoder):
    """Custom Airflow json encoder implementation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default = self._default

    @staticmethod
    def _default(obj):
        """Convert dates and numpy objects in a json serializable format."""
        if isinstance(obj, datetime):
            if is_naive(obj):
                obj = convert_to_utc(obj)
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, Decimal):
            _, _, exponent = obj.as_tuple()
            if exponent >= 0:  # No digits after the decimal point.
                return int(obj)
            # Technically lossy due to floating point errors, but the best we
            # can do without implementing a custom encode function.
            return float(obj)
        elif np is not None and isinstance(
            obj,
            (
                np.int_,
                np.intc,
                np.intp,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
                np.uint8,
                np.uint16,
                np.uint32,
                np.uint64,
            ),
        ):
            return int(obj)
        elif np is not None and isinstance(obj, np.bool_):
            return bool(obj)
        elif np is not None and isinstance(
            obj, (np.float_, np.float16, np.float32, np.float64, np.complex_, np.complex64, np.complex128)
        ):
            return float(obj)
        elif k8s is not None and isinstance(obj, (k8s.V1Pod, k8s.V1ResourceRequirements)):
            from airflow.kubernetes.pod_generator import PodGenerator

            def safe_get_name(pod):
                """
                We're running this in an except block, so we don't want it to
                fail under any circumstances, e.g. by accessing an attribute that isn't there
                """
                try:
                    return pod.metadata.name
                except Exception:
                    return None

            try:
                return PodGenerator.serialize_pod(obj)
            except Exception:
                log.warning("JSON encoding failed for pod %s", safe_get_name(obj))
                log.debug("traceback for pod JSON encode error", exc_info=True)
                return {}

        raise TypeError(f"Object of type '{obj.__class__.__qualname__}' is not JSON serializable")


class AirflowJsonProvider(JSONProvider):
    """JSON Provider for Flask app to use AirflowJsonEncoder."""

    ensure_ascii: bool = True
    sort_keys: bool = True

    def dumps(self, obj, **kwargs):
        kwargs.setdefault("ensure_ascii", self.ensure_ascii)
        kwargs.setdefault("sort_keys", self.sort_keys)
        return json.dumps(obj, **kwargs, cls=AirflowJsonEncoder)

    def loads(self, s: str | bytes, **kwargs):
        return json.loads(s, **kwargs)


# for now separate as AirflowJsonEncoder is non-standard
class XComEncoder(json.JSONEncoder):
    """This encoder serializes any object that has attr, dataclass or a custom serializer."""

    def default(self, o: object) -> dict:
        from airflow.serialization.serialized_objects import BaseSerialization

        dct = {
            CLASSNAME: o.__module__ + "." + o.__class__.__qualname__,
            VERSION: getattr(o.__class__, "version", DEFAULT_VERSION),
        }

        if hasattr(o, "serialize"):
            dct[DATA] = getattr(o, "serialize")()
            return dct
        elif dataclasses.is_dataclass(o.__class__):
            data = dataclasses.asdict(o)
            dct[DATA] = BaseSerialization.serialize(data)
            return dct
        elif attr.has(o.__class__):
            # Only include attributes which we can pass back to the classes constructor
            data = attr.asdict(o, recurse=True, filter=lambda a, v: a.init)  # type: ignore[arg-type]
            dct[DATA] = BaseSerialization.serialize(data)
            return dct
        else:
            return super().default(o)

    def encode(self, o: Any) -> str:
        if isinstance(o, dict) and CLASSNAME in o:
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

    @staticmethod
    def object_hook(dct: dict) -> object:
        dct = XComDecoder._convert(dct)

        if CLASSNAME in dct and VERSION in dct:
            from airflow.serialization.serialized_objects import BaseSerialization

            cls = import_string(dct[CLASSNAME])

            if hasattr(cls, "deserialize"):
                return getattr(cls, "deserialize")(dct[DATA], dct[VERSION])

            version = getattr(cls, "version", 0)
            if int(dct[VERSION]) > version:
                raise TypeError(
                    "serialized version of %s is newer than module version (%s > %s)",
                    dct[CLASSNAME],
                    dct[VERSION],
                    version,
                )

            if not attr.has(cls) and not dataclasses.is_dataclass(cls):
                raise TypeError(
                    f"cannot deserialize: no deserialization method "
                    f"for {dct[CLASSNAME]} and not attr/dataclass decorated"
                )

            return cls(**BaseSerialization.deserialize(dct[DATA]))

        return dct

    @staticmethod
    def orm_object_hook(dct: dict) -> object:
        """Creates a readable representation of a serialized object"""
        dct = XComDecoder._convert(dct)
        if CLASSNAME in dct and VERSION in dct:
            from airflow.serialization.serialized_objects import BaseSerialization

            if Encoding.VAR in dct[DATA] and Encoding.TYPE in dct[DATA]:
                data = BaseSerialization.deserialize(dct[DATA])
                if not isinstance(data, dict):
                    raise TypeError(f"deserialized value should be a dict, but is {type(data)}")
            else:
                # custom serializer
                data = dct[DATA]

            s = f"{dct[CLASSNAME]}@version={dct[VERSION]}("
            for k, v in data.items():
                s += f"{k}={v},"
            s = s[:-1] + ")"
            return s

        return dct

    @staticmethod
    def _convert(old: dict) -> dict:
        """Converts an old style serialization to new style"""
        if OLD_TYPE in old and OLD_SOURCE in old:
            return {CLASSNAME: old[OLD_TYPE], VERSION: DEFAULT_VERSION, DATA: old[OLD_DATA]}

        return old
