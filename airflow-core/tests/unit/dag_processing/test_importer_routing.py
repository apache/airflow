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
from unittest import mock

from airflow.dag_processing.importer_routing import get_claiming_importer

from tests_common.test_utils.config import conf_vars
from unit.dag_processing.fake_lang_sdk import FakeCoordinator, FakeCoordinatorDagImporter, fake_coordinator


def test_get_claiming_importer_returns_the_importer_that_claims_the_file(tmp_path):
    with fake_coordinator():
        importer = get_claiming_importer(tmp_path / "dags.native", "testing")
        others = [get_claiming_importer(tmp_path / name, "testing") for name in ("dags.other", "dag.py")]

    assert isinstance(importer, FakeCoordinatorDagImporter)
    assert importer.bundle_name == "testing"
    assert others == [None, None]


def test_get_claiming_importer_claims_a_file_that_several_coordinators_could_parse(tmp_path):
    with fake_coordinator("first", "second"):
        importer = get_claiming_importer(tmp_path / "dags.native", "testing")

    assert isinstance(importer, FakeCoordinatorDagImporter)


@mock.patch(
    "airflow.sdk.coordinators._dag_importer.COORDINATOR_DAG_IMPORTERS", ("nonexistent.module.Importer",)
)
def test_get_claiming_importer_logs_a_broken_configuration(tmp_path, caplog):
    spec = {"classpath": f"{FakeCoordinator.__module__}.FakeCoordinator", "kwargs": {}}
    with (
        conf_vars({("sdk", "coordinators"): json.dumps({"fake": spec})}),
        caplog.at_level(logging.ERROR, logger="airflow.dag_processing.importer_routing"),
    ):
        assert get_claiming_importer(tmp_path / "dags.native", "testing") is None

    [record] = [r for r in caplog.records if r.name == "airflow.dag_processing.importer_routing"]
    assert (
        record.getMessage()
        == f"Cannot load the Dag importer for {tmp_path / 'dags.native'} in bundle testing"
    )
    assert "Cannot build the coordinator Dag importers of Dag bundle 'testing'" in str(record.exc_info[1])
