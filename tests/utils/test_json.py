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

import decimal
import json
from datetime import date, datetime

import numpy as np
import pendulum
import pytest

from airflow.utils import json as utils_json


class TestAirflowJsonEncoder:
    def test_encode_datetime(self):
        obj = datetime.strptime("2017-05-21 00:00:00", "%Y-%m-%d %H:%M:%S")
        assert json.dumps(obj, cls=utils_json.AirflowJsonEncoder) == '"2017-05-21T00:00:00+00:00"'

    def test_encode_pendulum(self):
        obj = pendulum.datetime(2017, 5, 21, tz="Asia/Kolkata")
        assert json.dumps(obj, cls=utils_json.AirflowJsonEncoder) == '"2017-05-21T00:00:00+05:30"'

    def test_encode_date(self):
        assert json.dumps(date(2017, 5, 21), cls=utils_json.AirflowJsonEncoder) == '"2017-05-21"'

    @pytest.mark.parametrize(
        "expr, expected",
        [("1", "1"), ("52e4", "520000"), ("2e0", "2"), ("12e-2", "0.12"), ("12.34", "12.34")],
    )
    def test_encode_decimal(self, expr, expected):
        assert json.dumps(decimal.Decimal(expr), cls=utils_json.AirflowJsonEncoder) == expected

    def test_encode_numpy_int(self):
        assert json.dumps(np.int32(5), cls=utils_json.AirflowJsonEncoder) == "5"

    def test_encode_numpy_bool(self):
        assert json.dumps(np.bool_(True), cls=utils_json.AirflowJsonEncoder) == "true"

    def test_encode_numpy_float(self):
        assert json.dumps(np.float16(3.76953125), cls=utils_json.AirflowJsonEncoder) == "3.76953125"

    def test_encode_k8s_v1pod(self):
        from kubernetes.client import models as k8s

        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo",
                namespace="bar",
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="foo",
                        image="bar",
                    )
                ]
            ),
        )
        assert json.loads(json.dumps(pod, cls=utils_json.AirflowJsonEncoder)) == {
            "metadata": {"name": "foo", "namespace": "bar"},
            "spec": {"containers": [{"image": "bar", "name": "foo"}]},
        }

    def test_encode_raises(self):
        with pytest.raises(TypeError, match="^.*is not JSON serializable$"):
            json.dumps(
                Exception,
                cls=utils_json.AirflowJsonEncoder,
            )
