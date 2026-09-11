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
"""Stub Dags for the Java runner-behaviour E2E fixtures in the java-test-bundle."""

from __future__ import annotations

from airflow.sdk import dag, task


@task.stub(queue="java-test")
def missing_no_arg_constructor(): ...


@task.stub(queue="java-test")
def non_static_inner(): ...


@dag(dag_id="java_uninstantiable")
def java_uninstantiable():
    missing_no_arg_constructor()
    non_static_inner()


java_uninstantiable()
