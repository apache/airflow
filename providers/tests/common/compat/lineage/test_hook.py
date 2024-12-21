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

from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


def test_that_compat_does_not_raise():
    # On compat tests this goes into ImportError code path
    assert get_hook_lineage_collector() is not None
    assert get_hook_lineage_collector() is not None


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
def test_compat_has_only_asset_methods():
    hook_lienage_collector = get_hook_lineage_collector()

    assert hook_lienage_collector.add_input_asset is not None
    assert hook_lienage_collector.add_output_asset is not None

    with pytest.raises(AttributeError):
        hook_lienage_collector.add_input_dataset
    with pytest.raises(AttributeError):
        hook_lienage_collector.add_output_dataset


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow < 3.0")
def test_compat_has_asset_and_dataset_methods():
    hook_lienage_collector = get_hook_lineage_collector()

    assert hook_lienage_collector.add_input_asset is not None
    assert hook_lienage_collector.add_output_asset is not None
    assert hook_lienage_collector.add_input_dataset is not None
    assert hook_lienage_collector.add_output_dataset is not None
