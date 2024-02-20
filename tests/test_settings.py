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

from unittest.mock import patch

from airflow import settings


@patch("airflow.settings.conf")
@patch.multiple("airflow.settings", SQL_ALCHEMY_V1=True)
def test_encoding_present(mock_conf):
    mock_conf.getjson.return_value = {}

    engine_args = settings.prepare_engine_args()

    assert "encoding" in engine_args


@patch("airflow.settings.conf")
@patch.multiple("airflow.settings", SQL_ALCHEMY_V1=False)
def test_encoding_absent(mock_conf):
    mock_conf.getjson.return_value = {}

    engine_args = settings.prepare_engine_args()

    assert "encoding" not in engine_args
