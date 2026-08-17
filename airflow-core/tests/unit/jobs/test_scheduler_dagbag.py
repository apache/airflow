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

import math

import pytest
from cachetools import LRUCache, TTLCache

from airflow.jobs.scheduler_dagbag import SchedulerDBDagBag

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestSchedulerDBDagBag:
    @pytest.mark.parametrize(
        ("cache_size", "cache_ttl", "expected_dags_type", "expected_maxsize"),
        [
            pytest.param(None, None, TTLCache, math.inf, id="defaults_ttl_only"),
            pytest.param("512", "3600", TTLCache, 512, id="size_and_ttl"),
            pytest.param("512", "0", LRUCache, 512, id="ttl_zero_lru_only"),
            pytest.param("0", "0", dict, None, id="both_zero_no_eviction"),
        ],
    )
    def test_from_config(self, cache_size, cache_ttl, expected_dags_type, expected_maxsize):
        overrides = {}
        if cache_size is not None:
            overrides[("scheduler", "dag_cache_size")] = cache_size
        if cache_ttl is not None:
            overrides[("scheduler", "dag_cache_ttl")] = cache_ttl

        with conf_vars(overrides):
            dag_bag = SchedulerDBDagBag.from_config()

        assert isinstance(dag_bag._dags, expected_dags_type)
        assert dag_bag._use_cache is (expected_dags_type is not dict)
        if expected_maxsize is not None:
            assert dag_bag._dags.maxsize == expected_maxsize

    def test_from_config_does_not_load_op_links(self):
        """Operator extra links are an API-server concern; the scheduler must not deserialize them."""
        assert SchedulerDBDagBag.from_config().load_op_links is False
