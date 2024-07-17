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

from unittest.mock import MagicMock, patch

import pytest

from airflow import plugins_manager
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.lineage import hook
from airflow.lineage.hook import (
    HookLineage,
    HookLineageCollector,
    HookLineageReader,
    NoOpCollector,
    get_hook_lineage_collector,
)
from tests.test_utils.mock_plugins import mock_plugin_manager


class TestHookLineageCollector:
    def test_are_datasets_collected(self):
        lineage_collector = HookLineageCollector()
        assert lineage_collector is not None
        assert lineage_collector.collected_datasets == HookLineage()
        input_hook = BaseHook()
        output_hook = BaseHook()
        lineage_collector.add_input_dataset(input_hook, uri="s3://in_bucket/file")
        lineage_collector.add_output_dataset(
            output_hook, uri="postgres://example.com:5432/database/default/table"
        )
        assert lineage_collector.collected_datasets == HookLineage(
            [(Dataset("s3://in_bucket/file"), input_hook)],
            [(Dataset("postgres://example.com:5432/database/default/table"), output_hook)],
        )

    @patch("airflow.lineage.hook.Dataset")
    def test_add_input_dataset(self, mock_dataset):
        collector = HookLineageCollector()
        dataset = MagicMock(spec=Dataset)
        mock_dataset.return_value = dataset

        hook = MagicMock()
        collector.add_input_dataset(hook, uri="test_uri")

        assert collector.inputs == [(dataset, hook)]
        mock_dataset.assert_called_once_with(uri="test_uri", extra=None)

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_dataset(self, mock_providers_manager):
        def create_dataset(arg1, arg2="default", extra=None):
            return Dataset(uri=f"myscheme://{arg1}/{arg2}", extra=extra)

        mock_providers_manager.return_value.dataset_factories = {"myscheme": create_dataset}
        collector = HookLineageCollector()
        assert collector.create_dataset(
            scheme="myscheme", uri=None, dataset_kwargs={"arg1": "value_1"}, dataset_extra=None
        ) == Dataset("myscheme://value_1/default")
        assert collector.create_dataset(
            scheme="myscheme",
            uri=None,
            dataset_kwargs={"arg1": "value_1", "arg2": "value_2"},
            dataset_extra={"key": "value"},
        ) == Dataset("myscheme://value_1/value_2", extra={"key": "value"})

    def test_collected_datasets(self):
        collector = HookLineageCollector()
        inputs = [(MagicMock()), MagicMock(spec=Dataset)]
        outputs = [(MagicMock()), MagicMock(spec=Dataset)]
        collector.inputs = inputs
        collector.outputs = outputs

        hook_lineage = collector.collected_datasets
        assert hook_lineage.inputs == inputs
        assert hook_lineage.outputs == outputs

    def test_has_collected(self):
        collector = HookLineageCollector()
        assert not collector.has_collected

        collector.inputs = [MagicMock(spec=Dataset), MagicMock()]
        assert collector.has_collected


class FakePlugin(plugins_manager.AirflowPlugin):
    name = "FakePluginHavingHookLineageCollector"
    hook_lineage_readers = [HookLineageReader]


@pytest.mark.parametrize(
    "has_readers, expected_class",
    [
        (True, HookLineageCollector),
        (False, NoOpCollector),
    ],
)
def test_get_hook_lineage_collector(has_readers, expected_class):
    # reset global variable
    hook._hook_lineage_collector = None
    plugins = [FakePlugin()] if has_readers else []
    with mock_plugin_manager(plugins=plugins):
        assert isinstance(get_hook_lineage_collector(), expected_class)
        assert get_hook_lineage_collector() is get_hook_lineage_collector()
