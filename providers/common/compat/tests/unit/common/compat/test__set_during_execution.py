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

from airflow.providers.common.compat import sdk

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

pytestmark = pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="The stand-in is only used on Airflow 2")

if not AIRFLOW_V_3_0_PLUS:
    from airflow.providers.common.compat._set_during_execution import SET_DURING_EXECUTION
    from airflow.serialization.helpers import serialize_template_field
    from airflow.utils.types import ArgNotSet


def test_compat_sdk_hands_out_the_stand_in():
    assert sdk.SET_DURING_EXECUTION is SET_DURING_EXECUTION


def test_is_an_arg_not_set_sentinel():
    assert isinstance(SET_DURING_EXECUTION, ArgNotSet)


def test_serializes_as_the_airflow_3_sentinel_does():
    """A bare ``NOTSET`` serializes as an object address, which differs in every process."""
    assert serialize_template_field(SET_DURING_EXECUTION, "prompt") == "DYNAMIC (set during execution)"
