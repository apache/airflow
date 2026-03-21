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

import itertools
import re
from typing import TYPE_CHECKING

import pytest

from airflow.exceptions import AirflowException
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.serialization.definitions.notset import NOTSET
from airflow.utils import helpers
from airflow.utils.helpers import (
    at_most_one,
    build_airflow_dagrun_url,
    exactly_one,
    merge_dicts,
    prune_dict,
    validate_key,
)

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

if TYPE_CHECKING:
    from airflow.jobs.job import Job

CHUNK_SIZE_POSITIVE_INT = "Chunk size must be a positive integer"


@pytest.fixture
def clear_db():
    clear_db_runs()
    clear_db_dags()
    yield
    clear_db_runs()
    clear_db_dags()


class TestHelpers:
    def test_chunks(self):
        with pytest.raises(ValueError, match=CHUNK_SIZE_POSITIVE_INT):
            list(helpers.chunks([1, 2, 3], 0))

        with pytest.raises(ValueError, match=CHUNK_SIZE_POSITIVE_INT):
            list(helpers.chunks([1, 2, 3], -3))

        assert list(helpers.chunks([], 5)) == []
        assert list(helpers.chunks([1], 1)) == [[1]]
        assert list(helpers.chunks([1, 2, 3], 2)) == [[1, 2], [3]]

    def test_is_container(self):
        assert not helpers.is_container("a string is not a container")
        assert helpers.is_container(["a", "list", "is", "a", "container"])

        assert helpers.is_container(["test_list"])
        assert not helpers.is_container("test_str_not_iterable")
        # Pass an object that is not iter nor a string.
        assert not helpers.is_container(10)

    def test_convert_camel_to_snake(self):
        assert helpers.convert_camel_to_snake("LocalTaskJob") == "local_task_job"
        assert helpers.convert_camel_to_snake("somethingVeryRandom") == "something_very_random"

    def test_merge_dicts(self):
        """
        Test _merge method from JSONFormatter
        """
        dict1 = {"a": 1, "b": 2, "c": 3}
        dict2 = {"a": 1, "b": 3, "d": 42}
        merged = merge_dicts(dict1, dict2)
        assert merged == {"a": 1, "b": 3, "c": 3, "d": 42}

    def test_merge_dicts_recursive_overlap_l1(self):
        """
        Test merge_dicts with recursive dict; one level of nesting
        """
        dict1 = {"a": 1, "r": {"a": 1, "b": 2}}
        dict2 = {"a": 1, "r": {"c": 3, "b": 0}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {"a": 1, "r": {"a": 1, "b": 0, "c": 3}}

    def test_merge_dicts_recursive_overlap_l2(self):
        """
        Test merge_dicts with recursive dict; two levels of nesting
        """
        dict1 = {"a": 1, "r": {"a": 1, "b": {"a": 1}}}
        dict2 = {"a": 1, "r": {"c": 3, "b": {"b": 1}}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {"a": 1, "r": {"a": 1, "b": {"a": 1, "b": 1}, "c": 3}}

    def test_merge_dicts_recursive_right_only(self):
        """
        Test merge_dicts with recursive when dict1 doesn't have any nested dict
        """
        dict1 = {"a": 1}
        dict2 = {"a": 1, "r": {"c": 3, "b": 0}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {"a": 1, "r": {"b": 0, "c": 3}}

    def test_build_airflow_dagrun_url(self):
        expected_url = "/dags/somedag/runs/abc123"
        assert build_airflow_dagrun_url(dag_id="somedag", run_id="abc123") == expected_url

    @pytest.mark.parametrize(
        ("key_id", "message", "exception"),
        [
            (3, "The key has to be a string and is <class 'int'>:3", TypeError),
            (None, "The key has to be a string and is <class 'NoneType'>:None", TypeError),
            ("simple_key", None, None),
            ("simple-key", None, None),
            ("group.simple_key", None, None),
            ("root.group.simple-key", None, None),
            (
                "key with space",
                "The key 'key with space' has to be made of alphanumeric "
                "characters, dashes, dots and underscores exclusively",
                AirflowException,
            ),
            (
                "key_with_!",
                "The key 'key_with_!' has to be made of alphanumeric "
                "characters, dashes, dots and underscores exclusively",
                AirflowException,
            ),
            (" " * 251, f"The key: {' ' * 251} has to be less than 250 characters", AirflowException),
        ],
    )
    def test_validate_key(self, key_id, message, exception):
        if message:
            with pytest.raises(exception, match=re.escape(message)):
                validate_key(key_id)
        else:
            validate_key(key_id)

    def test_exactly_one(self):
        """
        Checks that when we set ``true_count`` elements to "truthy", and others to "falsy",
        we get the expected return.

        We check for both True / False, and truthy / falsy values 'a' and '', and verify that
        they can safely be used in any combination.
        """

        def assert_exactly_one(true=0, truthy=0, false=0, falsy=0):
            sample = []
            for truth_value, num in [(True, true), (False, false), ("a", truthy), ("", falsy)]:
                if num:
                    sample.extend([truth_value] * num)
            if sample:
                expected = true + truthy == 1
                assert exactly_one(*sample) is expected

        for row in itertools.product(range(4), repeat=4):
            assert_exactly_one(*row)

    def test_exactly_one_should_fail(self):
        with pytest.raises(ValueError, match="Not supported for iterable args"):
            exactly_one([True, False])

    def test_at_most_one(self):
        """
        Checks that when we set ``true_count`` elements to "truthy", and others to "falsy",
        we get the expected return.
        We check for both True / False, and truthy / falsy values 'a' and '', and verify that
        they can safely be used in any combination.
        NOTSET values should be ignored.
        """

        def assert_at_most_one(true=0, truthy=0, false=0, falsy=0, notset=0):
            sample = []
            for truth_value, num in [
                (True, true),
                (False, false),
                ("a", truthy),
                ("", falsy),
                (NOTSET, notset),
            ]:
                if num:
                    sample.extend([truth_value] * num)
            if sample:
                expected = true + truthy in (0, 1)
                assert at_most_one(*sample) is expected

        for row in itertools.product(range(4), repeat=4):
            print(row)
            assert_at_most_one(*row)

    @pytest.mark.parametrize(
        ("mode", "expected"),
        [
            (
                "strict",
                {
                    "b": "",
                    "c": {"b": "", "c": "hi", "d": ["", 0, "1"]},
                    "d": ["", 0, "1"],
                    "e": ["", 0, {"b": "", "c": "hi", "d": ["", 0, "1"]}, ["", 0, "1"], [""]],
                    "f": {},
                    "g": [""],
                },
            ),
            (
                "truthy",
                {
                    "c": {"c": "hi", "d": ["1"]},
                    "d": ["1"],
                    "e": [{"c": "hi", "d": ["1"]}, ["1"]],
                },
            ),
        ],
    )
    def test_prune_dict(self, mode, expected):
        l1 = ["", 0, "1", None]
        d1 = {"a": None, "b": "", "c": "hi", "d": l1}
        d2 = {"a": None, "b": "", "c": d1, "d": l1, "e": [None, "", 0, d1, l1, [""]], "f": {}, "g": [""]}
        assert prune_dict(d2, mode=mode) == expected


class MockJobRunner(BaseJobRunner):
    job_type = "MockJob"

    def __init__(self, job: Job, func=None):
        super().__init__(job)
        self.job = job
        self.job.job_type = self.job_type
        self.func = func

    def _execute(self):
        if self.func is not None:
            return self.func()
        return None


class SchedulerJobRunner(MockJobRunner):
    job_type = "SchedulerJob"


class TriggererJobRunner(MockJobRunner):
    job_type = "TriggererJob"
