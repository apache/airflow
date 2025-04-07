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

import pytest

from airflow.sdk.definitions.taskgroup import TaskGroup


class TestTaskGroup:
    @pytest.mark.parametrize(
        "group_id, exc_type, exc_value",
        [
            pytest.param(
                123,
                TypeError,
                "The key has to be a string and is <class 'int'>:123",
                id="type",
            ),
            pytest.param(
                "a" * 1000,
                ValueError,
                "The key has to be less than 200 characters, not 1000",
                id="long",
            ),
            pytest.param(
                "something*invalid",
                ValueError,
                "The key 'something*invalid' has to be made of alphanumeric characters, dashes, "
                "and underscores exclusively",
                id="illegal",
            ),
        ],
    )
    def test_dag_id_validation(self, group_id, exc_type, exc_value):
        with pytest.raises(exc_type) as ctx:
            TaskGroup(group_id)
        assert str(ctx.value) == exc_value
