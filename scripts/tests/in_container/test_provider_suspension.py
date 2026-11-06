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
from provider_suspension import remove_suspended_doc_urls, remove_suspended_import_errors

BEAM_URL = "/docs/apache-airflow-providers-apache-beam/operators.rst"
YANDEX_URL = "/docs/apache-airflow-providers-yandex/operators/dataproc.rst"
GOOGLE_URL = "/docs/apache-airflow-providers-google/operators/bigquery.rst"
ALL_URLS = [BEAM_URL, YANDEX_URL, GOOGLE_URL]

BEAM_ERROR = "No module named 'airflow.providers.apache.beam'"
YANDEX_ERROR = "No module named 'airflow.providers.yandex'"
OTHER_ERROR = "Provider google has an invalid integration"
ALL_ERRORS = [BEAM_ERROR, YANDEX_ERROR, OTHER_ERROR]


@pytest.mark.parametrize(
    "suspended_packages, expected",
    [
        pytest.param(set(), ALL_URLS, id="none-suspended"),
        pytest.param({"apache-airflow-providers-apache-beam"}, [YANDEX_URL, GOOGLE_URL], id="one-suspended"),
        pytest.param(
            {"apache-airflow-providers-apache-beam", "apache-airflow-providers-yandex"},
            [GOOGLE_URL],
            id="several-suspended",
        ),
    ],
)
def test_remove_suspended_doc_urls(suspended_packages, expected):
    assert remove_suspended_doc_urls(ALL_URLS, suspended_packages) == set(expected)


@pytest.mark.parametrize(
    "suspended_packages, expected",
    [
        pytest.param(set(), ALL_ERRORS, id="none-suspended"),
        pytest.param(
            {"apache-airflow-providers-apache-beam"}, [YANDEX_ERROR, OTHER_ERROR], id="one-suspended"
        ),
        pytest.param(
            {"apache-airflow-providers-apache-beam", "apache-airflow-providers-yandex"},
            [OTHER_ERROR],
            id="several-suspended",
        ),
    ],
)
def test_remove_suspended_import_errors(suspended_packages, expected):
    assert remove_suspended_import_errors(ALL_ERRORS, suspended_packages) == expected
