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
#

import unittest

from airflow.contrib.hooks import bigquery_hook as hook


class TestBigQueryTableSplitter(unittest.TestCase):
    def test_internal_need_default_project(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('dataset.table', None)

        self.assertIn('INTERNAL: No default project is specified',
                      str(context.exception), "")

    def test_split_dataset_table(self):
        project, dataset, table = hook._split_tablename('dataset.table',
                                                        'project')
        self.assertEqual("project", project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

    def test_split_project_dataset_table(self):
        project, dataset, table = hook._split_tablename('alternative:dataset.table',
                                                        'project')
        self.assertEqual("alternative", project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

    def test_sql_split_project_dataset_table(self):
        project, dataset, table = hook._split_tablename('alternative.dataset.table',
                                                        'project')
        self.assertEqual("alternative", project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

    def test_invalid_syntax_column_double_project(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt.dataset.table',
                                  'project')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertFalse('Format exception for' in str(context.exception))

    def test_invalid_syntax_double_column(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt:dataset.table',
                                  'project')

        self.assertIn('Expect format of (<project:)<dataset>.<table>',
                      str(context.exception), "")
        self.assertFalse('Format exception for' in str(context.exception))

    def test_invalid_syntax_tiple_dot(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1.alt.dataset.table',
                                  'project')

        self.assertIn('Expect format of (<project.|<project:)<dataset>.<table>',
                      str(context.exception), "")
        self.assertFalse('Format exception for' in str(context.exception))

    def test_invalid_syntax_column_double_project_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt.dataset.table',
                                  'project', 'var_x')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")

    def test_invalid_syntax_double_column_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt:dataset.table',
                                  'project', 'var_x')

        self.assertIn('Expect format of (<project:)<dataset>.<table>',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")

    def test_invalid_syntax_tiple_dot_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1.alt.dataset.table',
                                  'project', 'var_x')

        self.assertIn('Expect format of (<project.|<project:)<dataset>.<table>',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")

class TestBigQueryHookSourceFormat(unittest.TestCase):
    def test_invalid_source_format(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").run_load("test.test", "test_schema.json", ["test_data.json"], source_format="json")

        # since we passed 'json' in, and it's not valid, make sure it's present in the error string.
        self.assertIn("json", str(context.exception))


class TestBigQueryBaseCursor(unittest.TestCase):
    def test_invalid_schema_update_options(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=["THIS IS NOT VALID"]
                )
        self.assertIn("THIS IS NOT VALID", str(context.exception))

    def test_invalid_schema_update_and_write_disposition(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=['ALLOW_FIELD_ADDITION'],
                write_disposition='WRITE_EMPTY'
            )
        self.assertIn("schema_update_options is only", str(context.exception))

if __name__ == '__main__':
    unittest.main()
