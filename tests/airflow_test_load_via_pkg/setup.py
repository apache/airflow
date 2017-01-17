# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

setup(
    name="airflow-test-load-via-pkg",
    version="1.0.0",
    long_description=__doc__,
    packages=["airflow_test_load_via_pkg"],
    entry_points = {
        'airflow_test.dags': [
            'test_dag = airflow_test_load_via_pkg.test_dag:entrypoint_test_dag'
        ],
        'airflow_test.plugins': [
            'test_plugin = airflow_test_load_via_pkg.another_test_plugin:AnotherAirflowTestPlugin'
        ]
      },
    include_package_data=True,
    zip_safe=False,
)