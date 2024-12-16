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
    "module_path, attr_name, warning_message",
    (
        (
            "airflow",
            "Dataset",
            (
                "Import 'Dataset' directly from the airflow module is deprecated and will be removed in the future. "
                "Please import it from 'airflow.sdk.definitions.asset.Dataset'."
            ),
        ),
        (
            "airflow.datasets",
            "Dataset",
            (
                "Import 'airflow.dataset.Dataset' is deprecated and "
                "will be removed in the Airflow 3.2. Please import it from 'airflow.sdk.definitions.asset.Asset'."
            ),
        ),
        (
            "airflow.datasets",
            "DatasetAlias",
            (
                "Import 'airflow.dataset.DatasetAlias' is deprecated and "
                "will be removed in the Airflow 3.2. Please import it from 'airflow.sdk.definitions.asset.AssetAlias'."
            ),
        ),
        (
            "airflow.datasets",
            "expand_alias_to_datasets",
            (
                "Import 'airflow.dataset.expand_alias_to_datasets' is deprecated and "
                "will be removed in the Airflow 3.2. Please import it from 'airflow.models.asset.expand_alias_to_assets'."
            ),
        ),
        (
            "airflow.datasets.metadata",
            "Metadata",
            (
                "Import from the airflow.dataset module is deprecated and "
                "will be removed in the Airflow 3.2. Please import it from "
                "'airflow.sdk.definitions.asset.metadata'."
            ),
        ),
    ),
)
def test_backward_compat_import_before_airflow_3_2(module_path, attr_name, warning_message):
    with pytest.warns() as record:
        import importlib

        mod = importlib.import_module(module_path, __name__)
        getattr(mod, attr_name)

    assert record[0].category is DeprecationWarning
    assert str(record[0].message) == warning_message
