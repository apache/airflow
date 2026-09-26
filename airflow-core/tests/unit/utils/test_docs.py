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

from unittest import mock

import pytest

from airflow.utils.docs import get_docs_url


class TestGetDocsUrl:
    @pytest.mark.parametrize(
        ("version", "page", "expected_url"),
        [
            ("3.4.0", None, "https://airflow.apache.org/docs/apache-airflow/stable/"),
            (
                "3.4.0.dev0",
                "migrations-ref.html",
                "https://airflow.apache.org/docs/apache-airflow/stable/migrations-ref.html",
            ),
            ("3.4.0rc1", None, "https://airflow.apache.org/docs/apache-airflow/stable/"),
            ("3.3.1", None, "https://airflow.apache.org/docs/apache-airflow/3.3.1/"),
            (
                "3.2.0",
                "project.html",
                "https://airflow.apache.org/docs/apache-airflow/3.2.0/project.html",
            ),
            ("2.0.0.dev0", None, "https://airflow.apache.org/docs/apache-airflow/stable/"),
            ("1.10.10", None, "https://airflow.apache.org/docs/apache-airflow/1.10.10/"),
        ],
    )
    def test_should_return_link(self, version, page, expected_url):
        with (
            mock.patch("airflow.version.version", version),
            mock.patch("airflow.utils.docs.LATEST_PUBLISHED_AIRFLOW_VERSION", "3.3.1"),
        ):
            assert expected_url == get_docs_url(page)
