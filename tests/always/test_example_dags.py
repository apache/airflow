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

import os
import unittest
from glob import glob

from airflow.models import DagBag
from tests.test_utils.asserts import assert_queries_count

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)

NO_DB_QUERY_EXCEPTION = ["/airflow/example_dags/example_subdag_operator.py"]


class TestExampleDags(unittest.TestCase):
    def test_should_be_importable(self):
        example_dags = list(glob(f"{ROOT_FOLDER}/airflow/**/example_dags/example_*.py", recursive=True))
        self.assertNotEqual(0, len(example_dags))
        for filepath in example_dags:
            relative_filepath = os.path.relpath(filepath, ROOT_FOLDER)
            with self.subTest(f"File {relative_filepath} should contain dags"):
                dagbag = DagBag(
                    dag_folder=filepath,
                    include_examples=False,
                )
                self.assertEqual(0, len(dagbag.import_errors), f"import_errors={str(dagbag.import_errors)}")
                self.assertGreaterEqual(len(dagbag.dag_ids), 1)

    def test_should_not_do_database_queries(self):
        example_dags = glob(f"{ROOT_FOLDER}/airflow/**/example_dags/example_*.py", recursive=True)
        example_dags = [
            dag_file
            for dag_file in example_dags
            if any(not dag_file.endswith(e) for e in NO_DB_QUERY_EXCEPTION)
        ]
        self.assertNotEqual(0, len(example_dags))
        for filepath in example_dags:
            relative_filepath = os.path.relpath(filepath, ROOT_FOLDER)
            with self.subTest(f"File {relative_filepath} shouldn't do database queries"):
                with assert_queries_count(0):
                    DagBag(
                        dag_folder=filepath,
                        include_examples=False,
                    )
