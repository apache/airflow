# -*- coding: utf-8 -*-
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
#
from unittest import TestCase

from mock import Mock

from airflow.plugin.rest_api import AirflowRestyResolver


class TestAirflowRestyResolver(TestCase):
    def test_resolve_function_from_operation_id_experimental(self):
        resolver = AirflowRestyResolver('experimental')

        for expected, operation_id in (
            ({'foo': 'bar_dict'}, 'tests.plugin.rest_api.test_resource.dict_handler'),
            ({'foo': 'bar_json'}, 'tests.plugin.rest_api.test_resource.to_json_handler'),
            ({'foo': 'bar_obj'}, 'tests.plugin.rest_api.test_resource.obj_handler'),
            ([{'foo': 'bar_dict'}], 'tests.plugin.rest_api.test_resource.list_dict_handler'),
            ([{'foo': 'bar_json'}], 'tests.plugin.rest_api.test_resource.to_json_list_handler'),
            ([], 'tests.plugin.rest_api.test_resource.empty_list_handler')
        ):
            self.assertEqual(
                expected,
                resolver.resolve_function_from_operation_id(
                    operation_id
                )(),
                'Actual is not expected on \'{0}\''.format(operation_id)
            )

    def test_resolve_function_from_operation_id(self):

        method_handler = AirflowRestyResolver('test-version'). \
            resolve_function_from_operation_id(
            'tests.plugin.rest_api.test_resource.dict_handler'
        )

        self.assertEqual(
            {'foo': 'bar_dict'},
            method_handler(),
        )

    def test_resolveoperation_id_experimental(self):
        self.assertEqual(
            'airflow.api.common.experimental.delete_dag.delete_dag',
            AirflowRestyResolver('experimental').resolve_operation_id(
                Mock(path='/dags/{dag_id}', method='delete')
            )
        )

    def test_resolve_operation_id(self):
        resolver = AirflowRestyResolver('test-version')
        for handler, path in (
            (
                'airflow.plugin.rest_api.test-version.resource.get',
                '/resource/{variable}'
            ),
            (
                'airflow.plugin.rest_api.test-version.resource.sub-resource.list',
                '/resource/{variable}/sub-resource'
            )
        ):
            self.assertEqual(
                handler,
                resolver.resolve_operation_id(
                    Mock(path=path, method='get')
                )
            )
