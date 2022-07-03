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
import re
from itertools import product

import pytest

from airflow import AirflowException
from airflow.utils import helpers, timezone
from airflow.utils.helpers import (
    at_most_one,
    build_airflow_url_with_query,
    exactly_one,
    merge_dicts,
    prune_dict,
    validate_group_key,
    validate_key,
)
from airflow.utils.types import NOTSET
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs


@pytest.fixture()
def clear_db():
    clear_db_runs()
    clear_db_dags()
    yield
    clear_db_runs()
    clear_db_dags()


class TestHelpers:
    @pytest.mark.usefixtures("clear_db")
    def test_render_log_filename(self, create_task_instance):
        try_number = 1
        dag_id = 'test_render_log_filename_dag'
        task_id = 'test_render_log_filename_task'
        execution_date = timezone.datetime(2016, 1, 1)

        ti = create_task_instance(dag_id=dag_id, task_id=task_id, execution_date=execution_date)
        filename_template = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log"

        ts = ti.get_template_context()['ts']
        expected_filename = f"{dag_id}/{task_id}/{ts}/{try_number}.log"

        rendered_filename = helpers.render_log_filename(ti, try_number, filename_template)

        assert rendered_filename == expected_filename

    def test_chunks(self):
        with pytest.raises(ValueError):
            list(helpers.chunks([1, 2, 3], 0))

        with pytest.raises(ValueError):
            list(helpers.chunks([1, 2, 3], -3))

        assert list(helpers.chunks([], 5)) == []
        assert list(helpers.chunks([1], 1)) == [[1]]
        assert list(helpers.chunks([1, 2, 3], 2)) == [[1, 2], [3]]

    def test_reduce_in_chunks(self):
        assert helpers.reduce_in_chunks(lambda x, y: x + [y], [1, 2, 3, 4, 5], []) == [[1, 2, 3, 4, 5]]

        assert helpers.reduce_in_chunks(lambda x, y: x + [y], [1, 2, 3, 4, 5], [], 2) == [[1, 2], [3, 4], [5]]

        assert helpers.reduce_in_chunks(lambda x, y: x + y[0] * y[1], [1, 2, 3, 4], 0, 2) == 14

    def test_is_container(self):
        assert not helpers.is_container("a string is not a container")
        assert helpers.is_container(["a", "list", "is", "a", "container"])

        assert helpers.is_container(['test_list'])
        assert not helpers.is_container('test_str_not_iterable')
        # Pass an object that is not iter nor a string.
        assert not helpers.is_container(10)

    def test_as_tuple(self):
        assert helpers.as_tuple("a string is not a container") == ("a string is not a container",)

        assert helpers.as_tuple(["a", "list", "is", "a", "container"]) == (
            "a",
            "list",
            "is",
            "a",
            "container",
        )

    def test_as_tuple_iter(self):
        test_list = ['test_str']
        as_tup = helpers.as_tuple(test_list)
        assert tuple(test_list) == as_tup

    def test_as_tuple_no_iter(self):
        test_str = 'test_str'
        as_tup = helpers.as_tuple(test_str)
        assert (test_str,) == as_tup

    def test_convert_camel_to_snake(self):
        assert helpers.convert_camel_to_snake('LocalTaskJob') == 'local_task_job'
        assert helpers.convert_camel_to_snake('somethingVeryRandom') == 'something_very_random'

    def test_merge_dicts(self):
        """
        Test _merge method from JSONFormatter
        """
        dict1 = {'a': 1, 'b': 2, 'c': 3}
        dict2 = {'a': 1, 'b': 3, 'd': 42}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'b': 3, 'c': 3, 'd': 42}

    def test_merge_dicts_recursive_overlap_l1(self):
        """
        Test merge_dicts with recursive dict; one level of nesting
        """
        dict1 = {'a': 1, 'r': {'a': 1, 'b': 2}}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': 0}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'r': {'a': 1, 'b': 0, 'c': 3}}

    def test_merge_dicts_recursive_overlap_l2(self):
        """
        Test merge_dicts with recursive dict; two levels of nesting
        """

        dict1 = {'a': 1, 'r': {'a': 1, 'b': {'a': 1}}}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': {'b': 1}}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'r': {'a': 1, 'b': {'a': 1, 'b': 1}, 'c': 3}}

    def test_merge_dicts_recursive_right_only(self):
        """
        Test merge_dicts with recursive when dict1 doesn't have any nested dict
        """
        dict1 = {'a': 1}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': 0}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'r': {'b': 0, 'c': 3}}

    @conf_vars(
        {
            ("webserver", "dag_default_view"): "graph",
        }
    )
    def test_build_airflow_url_with_query(self):
        """
        Test query generated with dag_id and params
        """
        query = {"dag_id": "test_dag", "param": "key/to.encode"}
        expected_url = "/dags/test_dag/graph?param=key%2Fto.encode"

        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            assert build_airflow_url_with_query(query) == expected_url

    @pytest.mark.parametrize(
        "key_id, message, exception",
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
            (' ' * 251, "The key has to be less than 250 characters", AirflowException),
        ],
    )
    def test_validate_key(self, key_id, message, exception):
        if message:
            with pytest.raises(exception, match=re.escape(message)):
                validate_key(key_id)
        else:
            validate_key(key_id)

    @pytest.mark.parametrize(
        "key_id, message, exception",
        [
            (3, "The key has to be a string and is <class 'int'>:3", TypeError),
            (None, "The key has to be a string and is <class 'NoneType'>:None", TypeError),
            ("simple_key", None, None),
            ("simple-key", None, None),
            (
                "group.simple_key",
                "The key 'group.simple_key' has to be made of alphanumeric "
                "characters, dashes and underscores exclusively",
                AirflowException,
            ),
            (
                "root.group-name.simple_key",
                "The key 'root.group-name.simple_key' has to be made of alphanumeric "
                "characters, dashes and underscores exclusively",
                AirflowException,
            ),
            (
                "key with space",
                "The key 'key with space' has to be made of alphanumeric "
                "characters, dashes and underscores exclusively",
                AirflowException,
            ),
            (
                "key_with_!",
                "The key 'key_with_!' has to be made of alphanumeric "
                "characters, dashes and underscores exclusively",
                AirflowException,
            ),
            (' ' * 201, "The key has to be less than 200 characters", AirflowException),
        ],
    )
    def test_validate_group_key(self, key_id, message, exception):
        if message:
            with pytest.raises(exception, match=re.escape(message)):
                validate_group_key(key_id)
        else:
            validate_group_key(key_id)

    def test_exactly_one(self):
        """
        Checks that when we set ``true_count`` elements to "truthy", and others to "falsy",
        we get the expected return.

        We check for both True / False, and truthy / falsy values 'a' and '', and verify that
        they can safely be used in any combination.
        """

        def assert_exactly_one(true=0, truthy=0, false=0, falsy=0):
            sample = []
            for truth_value, num in [(True, true), (False, false), ('a', truthy), ('', falsy)]:
                if num:
                    sample.extend([truth_value] * num)
            if sample:
                expected = True if true + truthy == 1 else False
                assert exactly_one(*sample) is expected

        for row in product(range(4), range(4), range(4), range(4)):
            assert_exactly_one(*row)

    def test_exactly_one_should_fail(self):
        with pytest.raises(ValueError):
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
                ('a', truthy),
                ('', falsy),
                (NOTSET, notset),
            ]:
                if num:
                    sample.extend([truth_value] * num)
            if sample:
                expected = True if true + truthy in (0, 1) else False
                assert at_most_one(*sample) is expected

        for row in product(range(4), range(4), range(4), range(4), range(4)):
            print(row)
            assert_at_most_one(*row)

    @pytest.mark.parametrize(
        'mode, expected',
        [
            (
                'strict',
                {
                    'b': '',
                    'c': {'b': '', 'c': 'hi', 'd': ['', 0, '1']},
                    'd': ['', 0, '1'],
                    'e': ['', 0, {'b': '', 'c': 'hi', 'd': ['', 0, '1']}, ['', 0, '1'], ['']],
                },
            ),
            (
                'truthy',
                {
                    'c': {'c': 'hi', 'd': ['1']},
                    'd': ['1'],
                    'e': [{'c': 'hi', 'd': ['1']}, ['1']],
                },
            ),
        ],
    )
    def test_prune_dict(self, mode, expected):
        l1 = ['', 0, '1', None]
        d1 = {'a': None, 'b': '', 'c': 'hi', 'd': l1}
        d2 = {'a': None, 'b': '', 'c': d1, 'd': l1, 'e': [None, '', 0, d1, l1, ['']]}
        assert prune_dict(d2, mode=mode) == expected
