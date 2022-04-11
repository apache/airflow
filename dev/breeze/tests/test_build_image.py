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
from unittest.mock import patch

import pytest

from airflow_breeze.build_image.ci.build_ci_image import get_ci_image_build_params


@pytest.mark.parametrize(
    'parameters, expected_build_params, cached_values, written_cache_version',
    [
        ({}, {"python": "3.7"}, {}, False),  # default value no params
        ({"python": "3.8"}, {"python": "3.8"}, {}, "3.8"),  # default value override params
        ({}, {"python": "3.8"}, {'PYTHON_MAJOR_MINOR_VERSION': "3.8"}, False),  # value from cache
        (
            {"python": "3.9"},
            {"python": "3.9"},
            {'PYTHON_MAJOR_MINOR_VERSION': "3.8"},
            "3.9",
        ),  # override cache with passed param
    ],
)
def test_get_image_params(parameters, expected_build_params, cached_values, written_cache_version):
    with patch('airflow_breeze.utils.cache.read_from_cache_file') as read_from_cache_mock, patch(
        'airflow_breeze.utils.cache.check_if_cache_exists'
    ) as check_if_cache_exists_mock, patch(
        'airflow_breeze.utils.cache.write_to_cache_file'
    ) as write_to_cache_file_mock, patch(
        'airflow_breeze.utils.cache.check_cached_value_is_allowed'
    ) as check_cache_and_write_mock:
        check_if_cache_exists_mock.return_value = True
        check_cache_and_write_mock.side_effect = lambda cache_key, default_value: (
            cache_key in cached_values,
            cached_values[cache_key] if cache_key in cached_values else default_value,
        )
        read_from_cache_mock.side_effect = lambda param_name: cached_values.get(param_name)
        build_parameters = get_ci_image_build_params(parameters)
        for param, param_value in expected_build_params.items():
            assert getattr(build_parameters, param) == param_value
        if written_cache_version:
            write_to_cache_file_mock.assert_called_once_with(
                "PYTHON_MAJOR_MINOR_VERSION", written_cache_version, check_allowed_values=True
            )
        else:
            write_to_cache_file_mock.assert_not_called()
