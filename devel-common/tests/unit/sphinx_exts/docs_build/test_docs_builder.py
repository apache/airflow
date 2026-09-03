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

from sphinx_exts.docs_build.code_utils import AIRFLOW_CONTENT_ROOT_PATH
from sphinx_exts.docs_build.docs_builder import AirflowDocsBuilder, get_available_packages


def test_mypy_docs_package_is_available():
    assert "apache-airflow-mypy" in get_available_packages()


def test_mypy_docs_source_directory():
    builder = AirflowDocsBuilder(package_name="apache-airflow-mypy")

    assert builder._src_dir == AIRFLOW_CONTENT_ROOT_PATH / "dev" / "mypy" / "docs"
