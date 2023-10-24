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

from airflow_breeze.global_constants import ALL_SPECIAL_DOC_KEYS, get_available_documentation_packages

providers_prefix = "apache-airflow-providers-"


def get_docs_filter_name_from_short_hand(short_form_providers: tuple[str]):
    providers = []
    for short_form_provider in short_form_providers:
        if specific_doc := ALL_SPECIAL_DOC_KEYS.get(short_form_provider):
            providers.append(specific_doc)
            continue

        short_form_provider.split(".")
        parts = "-".join(short_form_provider.split("."))
        providers.append(providers_prefix + parts)
    return tuple(providers)


def expand_all_providers(short_doc_packages: tuple[str, ...]) -> tuple[str, ...]:
    if "all-providers" in short_doc_packages:
        packages = [package for package in short_doc_packages if package != "all-providers"]
        packages.extend(get_available_documentation_packages(only_providers=True, short_version=True))
        short_doc_packages = tuple(set(packages))
    return short_doc_packages
