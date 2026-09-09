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

from pathlib import Path
from unittest import mock

from airflow_breeze.utils.add_back_references import start_generating_back_references


@mock.patch("airflow_breeze.utils.add_back_references.generate_back_references", autospec=True)
def test_mypy_docs_do_not_generate_provider_back_references(mock_generate_back_references):
    package_ids = ["apache-airflow-mypy"]

    start_generating_back_references(Path("airflow-site"), package_ids)

    assert package_ids == []
    mock_generate_back_references.assert_not_called()
