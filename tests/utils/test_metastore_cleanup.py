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
from unittest.mock import MagicMock, patch

import pytest
from pytest import param

from airflow.utils.metastore_cleanup import config_dict, run_cleanup


class TestMetastoreCleanup:
    @pytest.mark.parametrize(
        'kwargs, called',
        [
            param(dict(confirm=True), True, id='true'),
            param(dict(), True, id='not supplied'),
            param(dict(confirm=False), False, id='false'),
        ],
    )
    @patch('airflow.utils.metastore_cleanup._cleanup_table')
    @patch('airflow.utils.metastore_cleanup._confirm_delete')
    def test_run_cleanup_confirm(self, confirm_delete_mock, clean_table_mock, kwargs, called):
        run_cleanup(
            clean_before_timestamp=None,
            table_names=None,
            dry_run=None,
            verbose=None,
            **kwargs,
        )
        if called:
            confirm_delete_mock.assert_called()
        else:
            confirm_delete_mock.assert_not_called()

    @pytest.mark.parametrize(
        'table_names',
        [
            ['xcom', 'log'],
            None,
        ],
    )
    @patch('airflow.utils.metastore_cleanup._cleanup_table')
    @patch('airflow.utils.metastore_cleanup._confirm_delete')
    def test_run_cleanup_tables(self, confirm_delete_mock, clean_table_mock, table_names):
        base_kwargs = dict(
            clean_before_timestamp=None,
            dry_run=None,
            verbose=None,
        )
        run_cleanup(**base_kwargs, table_names=table_names)
        assert clean_table_mock.call_count == len(table_names) if table_names else len(config_dict)

    @pytest.mark.parametrize(
        'dry_run',
        [None, True, False],
    )
    @patch('airflow.utils.metastore_cleanup._build_query', MagicMock())
    @patch('airflow.utils.metastore_cleanup._print_entities', MagicMock())
    @patch('airflow.utils.metastore_cleanup._do_delete')
    @patch('airflow.utils.metastore_cleanup._confirm_delete', MagicMock())
    def test_run_cleanup_dry_run(self, do_delete, dry_run):
        base_kwargs = dict(
            clean_before_timestamp=None,
            dry_run=dry_run,
            verbose=None,
        )
        run_cleanup(
            **base_kwargs,
        )
        if dry_run:
            do_delete.assert_not_called()
        else:
            do_delete.assert_called()
