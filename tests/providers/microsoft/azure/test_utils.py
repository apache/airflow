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

from tests.test_utils.providers import get_provider_min_airflow_version, object_exists


def test__ensure_prefixes_removal():
    """Ensure that _ensure_prefixes is removed from snowflake when airflow min version >= 2.5.0."""
    path = 'airflow.providers.microsoft.azure.utils._ensure_prefixes'
    if not object_exists(path):
        raise Exception(
            "You must remove this test. It only exists to "
            "remind us to remove decorator `_ensure_prefixes`."
        )

    if get_provider_min_airflow_version('apache-airflow-providers-microsoft-azure') >= (2, 5):
        raise Exception(
            "You must now remove `_ensure_prefixes` from azure utils."
            " The functionality is now taken care of by providers manager."
        )
