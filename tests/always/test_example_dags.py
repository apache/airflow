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

import os
from glob import glob

import pytest

from airflow.models import DagBag
from tests.test_utils.asserts import assert_queries_count

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)

NO_DB_QUERY_EXCEPTION = ["/airflow/example_dags/example_subdag_operator.py"]


def example_dags():
    example_dirs = ["airflow/**/example_dags/example_*.py", "tests/system/providers/**/example_*.py"]
    for example_dir in example_dirs:
        yield from glob(f"{ROOT_FOLDER}/{example_dir}", recursive=True)


def example_dags_except_db_exception():
    return [
        dag_file
        for dag_file in example_dags()
        if any(not dag_file.endswith(e) for e in NO_DB_QUERY_EXCEPTION)
    ]


def relative_path(path):
    return os.path.relpath(path, ROOT_FOLDER)


@pytest.mark.parametrize("example", example_dags(), ids=relative_path)
def test_should_be_importable(example):
    dagbag = DagBag(
        dag_folder=example,
        include_examples=False,
    )
    assert len(dagbag.import_errors) == 0, f"import_errors={str(dagbag.import_errors)}"
    assert len(dagbag.dag_ids) >= 1


@pytest.mark.parametrize("example", example_dags_except_db_exception(), ids=relative_path)
def test_should_not_do_database_queries(example):
    with assert_queries_count(0):
        DagBag(
            dag_folder=example,
            include_examples=False,
        )
