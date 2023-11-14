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

from airflow_breeze.global_constants import REGULAR_DOC_PACKAGES
from airflow_breeze.utils.packages import get_available_packages


def test_get_available_packages():
    assert len(get_available_packages()) > 70
    assert all(package not in REGULAR_DOC_PACKAGES for package in get_available_packages())


def test_get_available_packages_include_non_provider_doc_packages():
    all_packages_including_regular_docs = get_available_packages(include_non_provider_doc_packages=True)
    for package in REGULAR_DOC_PACKAGES:
        assert package in all_packages_including_regular_docs

    assert "all-providers" not in all_packages_including_regular_docs


def test_get_available_packages_include_non_provider_doc_packages_and_all_providers():
    all_packages_including_regular_docs = get_available_packages(
        include_non_provider_doc_packages=True, include_all_providers=True
    )
    for package in REGULAR_DOC_PACKAGES:
        assert package in all_packages_including_regular_docs

    assert "all-providers" in all_packages_including_regular_docs
