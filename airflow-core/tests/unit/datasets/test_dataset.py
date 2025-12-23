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


@pytest.mark.parametrize(
    ("module_path", "attr_name", "expected_value", "warning_message"),
    (
        pytest.param(
            "airflow",
            "Dataset",
            "airflow.sdk.definitions.asset.Asset",
            (
                "Import 'Dataset' directly from the airflow module is deprecated "
                "and will be removed in the future. Please import it from 'airflow.sdk.Asset'."
            ),
            id="airflow.Dataset",
        ),
        pytest.param(
            "airflow.datasets",
            "Dataset",
            "airflow.sdk.definitions.asset.Asset",
            (
                "Import 'airflow.datasets.Dataset' is deprecated and "
                "will be removed in Airflow 3.2. Please import it from 'airflow.sdk.Asset'."
            ),
            id="airflow.datasets.Dataset",
        ),
        pytest.param(
            "airflow.datasets",
            "DatasetAlias",
            "airflow.sdk.definitions.asset.AssetAlias",
            (
                "Import 'airflow.datasets.DatasetAlias' is deprecated and "
                "will be removed in Airflow 3.2. Please import it from 'airflow.sdk.AssetAlias'."
            ),
            id="airflow.datasets.DatasetAlias",
        ),
        pytest.param(
            "airflow.datasets",
            "expand_alias_to_datasets",
            "airflow.models.asset.expand_alias_to_assets",
            (
                "Import 'airflow.datasets.expand_alias_to_datasets' is deprecated and will be removed "
                "in Airflow 3.2. Please import it from 'airflow.models.asset.expand_alias_to_assets'."
            ),
            id="airflow.datasets.expand_alias_to_datasets",
        ),
        pytest.param(
            "airflow.datasets.metadata",
            "Metadata",
            "airflow.sdk.definitions.asset.metadata.Metadata",
            (
                "Import from the airflow.datasets.metadata module is deprecated and "
                "will be removed in Airflow 3.2. Please import it from "
                "'airflow.sdk'."
            ),
            id="airflow.datasets.metadata.Metadata",
        ),
    ),
)
def test_backward_compat_import_before_airflow_3_2(module_path, attr_name, expected_value, warning_message):
    import importlib

    with pytest.warns() as record:
        mod = importlib.import_module(module_path, __name__)
        attr = getattr(mod, attr_name)
    assert f"{attr.__module__}.{attr.__name__}" == expected_value
    from airflow.utils.deprecation_tools import DeprecatedImportWarning

    assert record[0].category is DeprecatedImportWarning
    assert str(record[0].message) == warning_message


# ruff: noqa: PT031
