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

from airflow.datasets import DatasetAlias
from airflow.models.dataset import DatasetAliasModel


class TestDatasetAliasModel:
    def test_from_public(self):
        dataset_alias = DatasetAlias(name="test_alias")
        dataset_alias_model = DatasetAliasModel.from_public(dataset_alias)

        assert dataset_alias_model.name == "test_alias"
