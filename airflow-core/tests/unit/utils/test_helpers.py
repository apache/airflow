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

import jinja2.exceptions
import pendulum
import pytest

from airflow.exceptions import AirflowException
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.serialization.definitions.notset import NOTSET
from airflow.utils import helpers
from airflow.utils.helpers import (
    at_most_one,
    build_airflow_dagrun_url,
    exactly_one,
    log_filename_template_renderer,
    merge_dicts,
    prune_dict,
    validate_key,
)
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import env_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

if TYPE_CHECKING:
    from airflow.jobs.job import Job

CHUNK_SIZE_POSITIVE_INT = "Chunk size must be a positive integer"
DEFAULT_DATE = pendulum.datetime(2016, 1, 1)


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
            (
                "my..key",
                "The key 'my..key' must not contain consecutive dots ('..') to prevent path traversal",
                AirflowException,
            ),
            (
                "..",
                "The key '..' must not contain consecutive dots ('..') to prevent path traversal",
                AirflowException,
            ),
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


# ``logical_date`` (DEFAULT_DATE, 2016) deliberately differs from ``run_after`` (RUN_AFTER, 2020) so
# the ``logical_date or run_after`` fallback is observable: a test using the same value for both could
# not tell the fallback path apart from the normal path.
RUN_AFTER = pendulum.datetime(2020, 5, 5, 6, 0)


@pytest.mark.db_test
class TestLogFilenameTemplateRenderer:
    """
    Tests for the scheduler-side log path renderer (:func:`log_filename_template_renderer`), #68075.

    The scheduler renders ``log_filename_template`` to decide where the worker writes a task's log,
    and the UI renders the same template (via :meth:`FileTaskHandler._render_filename`) to find that
    file again. The two must resolve to the same string, including for runs whose ``logical_date`` is
    ``None`` (asset-triggered / partitioned runs, AIP-83).
    """

    # The default log_filename_template contains a Jinja ``{% %}`` block; its raw ``%`` breaks
    # configparser interpolation when ``conf_vars`` restores it on teardown. ``env_vars`` sets the
    # template via an env var instead, which ``conf.get`` reads unprocessed.
    TEMPLATE_ENV = "AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE"

    def _make_ti(self, create_task_instance, logical_date):
        return create_task_instance(
            dag_id="dag_for_log_filename_rendering",
            task_id="task_for_log_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            run_after=RUN_AFTER,
            logical_date=logical_date,
        )

    def _render(self, template, ti):
        with env_vars({self.TEMPLATE_ENV: template}):
            log_filename_template_renderer.cache_clear()
            try:
                return log_filename_template_renderer()(ti=ti)
            finally:
                log_filename_template_renderer.cache_clear()

    def test_jinja_logical_date_none_renders_none(self, create_task_instance):
        """A plain ``{{ ti.logical_date }}`` stringifies a missing logical date to "None" without crashing."""
        ti = self._make_ti(create_task_instance, None)
        assert self._render("{{ ti.logical_date }}", ti) == "None"

    @pytest.mark.parametrize("logical_date", [None, DEFAULT_DATE])
    def test_jinja_ts_uses_logical_date_or_run_after(self, create_task_instance, logical_date):
        """``ts`` mirrors the reader: it is ``(logical_date or run_after).isoformat()``."""
        ti = self._make_ti(create_task_instance, logical_date)
        assert self._render("{{ ts }}", ti) == (logical_date or RUN_AFTER).isoformat()

    def test_fstring_logical_date_none_uses_run_after(self, create_task_instance):
        """An f-string ``{logical_date}`` falls back to ``run_after`` instead of dereferencing ``None``."""
        ti = self._make_ti(create_task_instance, None)
        assert self._render("{logical_date}", ti) == RUN_AFTER.isoformat()

    def test_fstring_extra_keys_available(self, create_task_instance):
        """``run_id`` and the data-interval keys are passed, so templates using them don't raise ``KeyError``."""
        ti = self._make_ti(create_task_instance, DEFAULT_DATE)
        rendered = self._render("{run_id}/{data_interval_start}/{data_interval_end}", ti)
        assert rendered.startswith(f"{ti.run_id}/")

    @pytest.mark.parametrize("logical_date", [None, DEFAULT_DATE])
    @pytest.mark.parametrize(
        "template",
        [
            "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
            "{dag_id}/{task_id}/{logical_date}/{run_id}"
            "/{data_interval_start}/{data_interval_end}/{try_number}.log",
        ],
    )
    def test_write_path_matches_read_path(
        self, create_task_instance, create_log_template, template, logical_date
    ):
        """
        The path the scheduler hands the worker to write to must equal the path the UI reads from.

        ``create_log_template`` must run before ``_make_ti`` so the new DagRun points at the template
        under test (``log_template_id`` defaults to the latest template id).
        """
        create_log_template(template)
        ti = self._make_ti(create_task_instance, logical_date)
        write_path = self._render(template, ti)
        read_path = FileTaskHandler("")._render_filename(ti, ti.try_number)
        assert write_path == read_path

    def test_jinja_method_on_none_logical_date_raises(self, create_task_instance):
        """
        Calling a method on a ``None`` logical_date is a user template error; the renderer does not
        rescue it (use ``ti.run_after.strftime(...)`` instead). This documents the boundary: ``None``
        renders as the string "None", but a broken template is allowed to raise rather than silently
        falling back to a different template.
        """
        ti = self._make_ti(create_task_instance, None)
        with pytest.raises(jinja2.exceptions.UndefinedError):
            self._render("{{ ti.logical_date.strftime('%Y') }}", ti)
