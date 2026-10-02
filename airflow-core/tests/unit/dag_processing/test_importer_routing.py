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

import json
import logging

from airflow.dag_processing.importer_routing import get_claiming_coordinator, warm_importer_registry

from tests_common.test_utils.config import conf_vars
from unit.dag_processing.fake_lang_sdk import FakeCoordinator, fake_coordinator


def test_get_claiming_coordinator_returns_the_coordinator_of_its_importer(tmp_path):
    with fake_coordinator():
        coordinator = get_claiming_coordinator(tmp_path / "dags.native", "testing")
        others = [get_claiming_coordinator(tmp_path / name, "testing") for name in ("dags.other", "dag.py")]

    assert isinstance(coordinator, FakeCoordinator)
    assert others == [None, None]


def test_get_claiming_coordinator_logs_a_broken_configuration(tmp_path, caplog):
    spec = {"classpath": f"{FakeCoordinator.__module__}.FakeCoordinator", "kwargs": {}}
    with (
        conf_vars({("sdk", "coordinators"): json.dumps({"first": spec, "second": spec})}),
        caplog.at_level(logging.ERROR, logger="airflow.dag_processing.importer_routing"),
    ):
        assert get_claiming_coordinator(tmp_path / "dags.native", "testing") is None

    [record] = caplog.records
    assert (
        record.getMessage()
        == f"Cannot load the Dag importer for {tmp_path / 'dags.native'} in bundle testing"
    )
    assert "Coordinators 'first' and 'second' both parse .native files" in str(record.exc_info[1])


def test_warm_importer_registry_logs_a_broken_configuration(caplog):
    spec = {"classpath": f"{FakeCoordinator.__module__}.FakeCoordinator", "kwargs": {}}
    with (
        conf_vars({("sdk", "coordinators"): json.dumps({"first": spec, "second": spec})}),
        caplog.at_level(logging.ERROR, logger="airflow.dag_processing.importer_routing"),
    ):
        warm_importer_registry("testing")

    assert [r.getMessage() for r in caplog.records] == [
        "Cannot build the Dag importer registry for bundle testing"
    ]
