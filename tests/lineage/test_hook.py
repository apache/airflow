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
from airflow.assets import Dataset
from airflow.hooks.base import BaseHook
from airflow.lineage import hook
from airflow.lineage.hook import (
    DatasetLineageInfo,
    HookLineage,
    HookLineageCollector,
    HookLineageReader,
    NoOpCollector,
    get_hook_lineage_collector,
)
from tests.test_utils.mock_plugins import mock_plugin_manager


class TestHookLineageCollector:
    def setup_method(self):
        self.collector = HookLineageCollector()

    def test_are_datasets_collected(self):
        assert self.collector is not None
        assert self.collector.collected_datasets == HookLineage()
        input_hook = BaseHook()
        output_hook = BaseHook()
        self.collector.add_input_dataset(input_hook, uri="s3://in_bucket/file")
        self.collector.add_output_dataset(
            output_hook, uri="postgres://example.com:5432/database/default/table"
        )
        assert self.collector.collected_datasets == HookLineage(
            [DatasetLineageInfo(dataset=Dataset("s3://in_bucket/file"), count=1, context=input_hook)],
            [
                DatasetLineageInfo(
                    dataset=Dataset("postgres://example.com:5432/database/default/table"),
                    count=1,
                    context=output_hook,
                )
            ],
        )

    @patch("airflow.lineage.hook.Dataset")
    def test_add_input_dataset(self, mock_dataset):
        dataset = MagicMock(spec=Dataset, extra={})
        mock_dataset.return_value = dataset

        hook = MagicMock()
        self.collector.add_input_dataset(hook, uri="test_uri")

        assert next(iter(self.collector._inputs.values())) == (dataset, hook)
        mock_dataset.assert_called_once_with(uri="test_uri", extra=None)

    def test_grouping_datasets(self):
        hook_1 = MagicMock()
        hook_2 = MagicMock()

        uri = "test://uri/"

        self.collector.add_input_dataset(context=hook_1, uri=uri)
        self.collector.add_input_dataset(context=hook_2, uri=uri)
        self.collector.add_input_dataset(context=hook_1, uri=uri, dataset_extra={"key": "value"})

        collected_inputs = self.collector.collected_datasets.inputs

        assert len(collected_inputs) == 3
        assert collected_inputs[0].dataset.uri == "test://uri/"
        assert collected_inputs[0].dataset == collected_inputs[1].dataset
        assert collected_inputs[0].count == 1
        assert collected_inputs[0].context == collected_inputs[2].context == hook_1
        assert collected_inputs[1].count == 1
        assert collected_inputs[1].context == hook_2
        assert collected_inputs[2].count == 1
        assert collected_inputs[2].dataset.extra == {"key": "value"}

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_dataset(self, mock_providers_manager):
        def create_dataset(arg1, arg2="default", extra=None):
            return Dataset(uri=f"myscheme://{arg1}/{arg2}", extra=extra)

        test_scheme = "myscheme"
        mock_providers_manager.return_value.dataset_factories = {test_scheme: create_dataset}

        test_uri = "urischeme://value_a/value_b"
        test_kwargs = {"arg1": "value_1"}
        test_kwargs_uri = "myscheme://value_1/default"
        test_extra = {"key": "value"}

        # test uri arg - should take precedence over the keyword args + scheme
        assert self.collector.create_dataset(
            scheme=test_scheme, uri=test_uri, dataset_kwargs=test_kwargs, dataset_extra=None
        ) == Dataset(test_uri)
        assert self.collector.create_dataset(
            scheme=test_scheme, uri=test_uri, dataset_kwargs=test_kwargs, dataset_extra={}
        ) == Dataset(test_uri)
        assert self.collector.create_dataset(
            scheme=test_scheme, uri=test_uri, dataset_kwargs=test_kwargs, dataset_extra=test_extra
        ) == Dataset(test_uri, extra=test_extra)

        # test keyword args
        assert self.collector.create_dataset(
            scheme=test_scheme, uri=None, dataset_kwargs=test_kwargs, dataset_extra=None
        ) == Dataset(test_kwargs_uri)
        assert self.collector.create_dataset(
            scheme=test_scheme, uri=None, dataset_kwargs=test_kwargs, dataset_extra={}
        ) == Dataset(test_kwargs_uri)
        assert self.collector.create_dataset(
            scheme=test_scheme,
            uri=None,
            dataset_kwargs={**test_kwargs, "arg2": "value_2"},
            dataset_extra=test_extra,
        ) == Dataset("myscheme://value_1/value_2", extra=test_extra)

        # missing both uri and scheme
        assert (
            self.collector.create_dataset(
                scheme=None, uri=None, dataset_kwargs=test_kwargs, dataset_extra=None
            )
            is None
        )

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_dataset_no_factory(self, mock_providers_manager):
        test_scheme = "myscheme"
        mock_providers_manager.return_value.dataset_factories = {}

        test_kwargs = {"arg1": "value_1"}

        assert (
            self.collector.create_dataset(
                scheme=test_scheme, uri=None, dataset_kwargs=test_kwargs, dataset_extra=None
            )
            is None
        )

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_dataset_factory_exception(self, mock_providers_manager):
        def create_dataset(extra=None, **kwargs):
            raise RuntimeError("Factory error")

        test_scheme = "myscheme"
        mock_providers_manager.return_value.dataset_factories = {test_scheme: create_dataset}

        test_kwargs = {"arg1": "value_1"}

        assert (
            self.collector.create_dataset(
                scheme=test_scheme, uri=None, dataset_kwargs=test_kwargs, dataset_extra=None
            )
            is None
        )

    def test_collected_datasets(self):
        context_input = MagicMock()
        context_output = MagicMock()

        self.collector.add_input_dataset(context_input, uri="test://input")
        self.collector.add_output_dataset(context_output, uri="test://output")

        hook_lineage = self.collector.collected_datasets
        assert len(hook_lineage.inputs) == 1
        assert hook_lineage.inputs[0].dataset.uri == "test://input/"
        assert hook_lineage.inputs[0].context == context_input

        assert len(hook_lineage.outputs) == 1
        assert hook_lineage.outputs[0].dataset.uri == "test://output/"

    def test_has_collected(self):
        collector = HookLineageCollector()
        assert not collector.has_collected

        collector._inputs = {"unique_key": (MagicMock(spec=Dataset), MagicMock())}
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
