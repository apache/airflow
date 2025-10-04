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

import numpy as np
import pytest

from airflow.sdk.execution_time.asset_helpers import normalize_asset_metadata


class TestNormalizeAssetMetadata:
    @pytest.mark.parametrize(
        "metadata, expected",
        [
            ({"np_int": np.int64(5)}, {"np_int": 5}),
            ({"np_float": np.float64(3.14)}, {"np_float": 3.14}),
            ({"np_bool": np.bool_(True)}, {"np_bool": True}),
            ({"np_array": np.array([1, 2, 3])}, {"np_array": [1, 2, 3]}),
            (
                {"np_complex": np.complex128(1 + 2j)},
                {"np_complex": {"real": 1.0, "imag": 2.0}},
            ),
            (
                {
                    "nested": {
                        "np_int": np.int64(5),
                        "list": [np.float64(3.14), np.array([1, 2])],
                    }
                },
                {"nested": {"np_int": 5, "list": [3.14, [1, 2]]}},
            ),
            (
                {"int": 1, "float": 2.0, "bool": True, "str": "test"},
                {"int": 1, "float": 2.0, "bool": True, "str": "test"},
            ),
        ],
    )
    def test_normalize_asset_metadata(self, metadata, expected):
        assert normalize_asset_metadata(metadata) == expected
