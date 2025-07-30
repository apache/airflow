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

import pytest
from fastapi import HTTPException

from airflow.api_fastapi.common.parameters import SortParam


class TestSortParam:
    def test_sort_param_max_number_of_filers(self):
        param = SortParam([], None, None)
        n_filters = param.MAX_SORT_PARAMS + 1
        param.value = [f"filter_{i}" for i in range(n_filters)]

        with pytest.raises(
            HTTPException,
            match=re.escape(
                f"400: Ordering with more than {param.MAX_SORT_PARAMS} parameters is not allowed. Provided: {param.value}"
            ),
        ):
            param.to_orm(None)
