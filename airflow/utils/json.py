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

from datetime import date, datetime
from decimal import Decimal

from flask.json import JSONEncoder

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore

try:
    from kubernetes.client import models as k8s
except ImportError:
    k8s = None

# Dates and JSON encoding/decoding


class AirflowJsonEncoder(JSONEncoder):
    """Custom Airflow json encoder implementation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default = self._default

    @staticmethod
    def _default(obj):
        """Convert dates and numpy objects in a json serializable format."""
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
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

            return PodGenerator.serialize_pod(obj)

        raise TypeError(f"Object of type '{obj.__class__.__name__}' is not JSON serializable")
