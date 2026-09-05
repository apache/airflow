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

from airflow_shared.configuration.exceptions import AirflowConfigException
from airflow_shared.module_loading.dag_importers import load_dag_importers


class DummyMethodImporter:
    @classmethod
    def supported_extensions(cls) -> list[str]:
        return [".custom"]


class DummyAttrImporter:
    supported_extensions = [".attr"]


class TestLoadDagImporters:
    def test_load_dag_importers_with_explicit_extensions(self):
        configs = [
            {
                "classpath": f"{__name__}.DummyAttrImporter",
                "extensions": ["py", ".ZIP"],
            }
        ]
        results = load_dag_importers(configs)
        assert len(results) == 1
        importer, extensions = results[0]
        assert isinstance(importer, DummyAttrImporter)
        assert extensions == [".py", ".zip"]

    @pytest.mark.parametrize(
        ("cls_name", "expected_exts"),
        [
            ("DummyMethodImporter", [".custom"]),
            ("DummyAttrImporter", [".attr"]),
        ],
    )
    def test_load_dag_importers_fallback_supported_extensions(self, cls_name, expected_exts):
        configs = [{"classpath": f"{__name__}.{cls_name}"}]
        results = load_dag_importers(configs)
        assert len(results) == 1
        importer, extensions = results[0]
        assert extensions == expected_exts

    @pytest.mark.parametrize(
        ("config", "error_match"),
        [
            (["not_a_dict"], "each entry must be a dictionary"),
            ([{"kwargs": {}}], "Missing required 'classpath'"),
            (
                [{"classpath": f"{__name__}.DummyAttrImporter", "kwargs": "not_a_dict"}],
                "must be a dictionary",
            ),
            (
                [{"classpath": f"{__name__}.DummyAttrImporter", "extensions": "not_a_list"}],
                "must be a list of strings",
            ),
            (
                [{"classpath": f"{__name__}.DummyAttrImporter", "extensions": [123]}],
                "must be a list of strings",
            ),
            ([{"classpath": "invalid.classpath.NonExistent"}], "Failed to load DAG importer"),
        ],
    )
    def test_load_dag_importers_validation_errors(self, config, error_match):
        with pytest.raises(AirflowConfigException, match=error_match):
            load_dag_importers(config)
