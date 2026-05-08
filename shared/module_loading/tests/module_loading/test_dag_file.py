#
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

from airflow_shared.module_loading import MODIFIED_DAG_MODULE_NAME, UNUSUAL_MODULE_PREFIX


def test_constants() -> None:
    """Test that the constants are as expected."""
    assert UNUSUAL_MODULE_PREFIX == "unusual_prefix_"
    assert MODIFIED_DAG_MODULE_NAME == "unusual_prefix_{path_hash}_{module_name}"
