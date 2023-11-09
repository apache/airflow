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

import os
from dataclasses import dataclass

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.utils.packages import get_long_package_names

providers_prefix = "apache-airflow-providers-"


@dataclass
class DocBuildParams:
    package_filter: tuple[str, ...]
    docs_only: bool
    spellcheck_only: bool
    short_doc_packages: tuple[str, ...]
    skip_environment_initialization: bool = False
    one_pass_only: bool = False
    github_actions = os.environ.get("GITHUB_ACTIONS", "false")

    @property
    def args_doc_builder(self) -> list[str]:
        doc_args = []
        if self.docs_only:
            doc_args.append("--docs-only")
        if self.spellcheck_only:
            doc_args.append("--spellcheck-only")
        if self.one_pass_only:
            doc_args.append("--one-pass-only")
        if AIRFLOW_BRANCH != "main":
            doc_args.append("--disable-provider-checks")
        if self.short_doc_packages:
            for filter_from_short_doc in get_long_package_names(self.short_doc_packages):
                doc_args.extend(["--package-filter", filter_from_short_doc])
        if self.package_filter:
            for filter in self.package_filter:
                doc_args.extend(["--package-filter", filter])
        return doc_args
