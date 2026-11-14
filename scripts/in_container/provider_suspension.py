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
"""Filters that drop what a suspended provider leaves behind in the provider.yaml checks."""

from __future__ import annotations

from collections.abc import Collection


def remove_suspended_doc_urls(doc_urls: Collection[str], suspended_packages: Collection[str]) -> set[str]:
    """Drop documentation urls that belong to one of the suspended providers."""
    return {url for url in doc_urls if not any(package in url for package in suspended_packages)}


def remove_suspended_import_errors(errors: Collection[str], suspended_packages: Collection[str]) -> list[str]:
    """Drop import errors raised for the modules of one of the suspended providers."""
    suspended_modules = {
        package.replace("apache-", "", 1).replace("-", ".") for package in suspended_packages
    }
    return [
        error
        for error in errors
        if not any(f"No module named '{module}'" in error for module in suspended_modules)
    ]
