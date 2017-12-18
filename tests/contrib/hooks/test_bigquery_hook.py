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
import mock

from airflow.contrib.hooks import bigquery_hook as hook
from oauth2client.contrib.gce import HttpAccessTokenRefreshError

bq_available = True

try:
    hook.BigQueryHook().get_service()
except HttpAccessTokenRefreshError:
    bq_available = False

class TestBigQueryDataframeResults(unittest.TestCase):
    def setUp(self):
        self.instance = hook.BigQueryHook()

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_output_is_dataframe_with_valid_query(self):
        import pandas as pd
        df = self.instance.get_pandas_df('select 1')
        self.assertIsInstance(df, pd.DataFrame)

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_throws_exception_with_invalid_query(self):
        with self.assertRaises(Exception) as context:
            self.instance.get_pandas_df('from `1`')
        self.assertIn('pandas_gbq.gbq.GenericGBQException: Reason: invalidQuery',
                      str(context.exception), "")

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_suceeds_with_explicit_legacy_query(self):
        df = self.instance.get_pandas_df('select 1', dialect='legacy')
        self.assertEqual(df.iloc(0)[0][0], 1)

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_suceeds_with_explicit_std_query(self):
        df = self.instance.get_pandas_df('select * except(b) from (select 1 a, 2 b)', dialect='standard')
        self.assertEqual(df.iloc(0)[0][0], 1)

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_throws_exception_with_incompatible_syntax(self):
        with self.assertRaises(Exception) as context:
            self.instance.get_pandas_df('select * except(b) from (select 1 a, 2 b)', dialect='legacy')
        self.assertIn('pandas_gbq.gbq.GenericGBQException: Reason: invalidQuery',
                      str(context.exception), "")

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

    def test_colon_in_project(self):
        project, dataset, table = hook._split_tablename('alt1:alt.dataset.table',
                                                        'project')

        self.assertEqual('alt1:alt', project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)


    def test_valid_double_column(self):
        project, dataset, table = hook._split_tablename('alt1:alt:dataset.table',
                                  'project')

        self.assertEqual('alt1:alt', project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)


    def test_invalid_syntax_triple_colon(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt2:alt3:dataset.table',
                                  'project')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertFalse('Format exception for' in str(context.exception))


    def test_invalid_syntax_triple_dot(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1.alt.dataset.table',
                                  'project')

        self.assertIn('Expect format of (<project.|<project:)<dataset>.<table>',
                      str(context.exception), "")
        self.assertFalse('Format exception for' in str(context.exception))

    def test_invalid_syntax_column_double_project_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt2:alt.dataset.table',
                                  'project', 'var_x')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")

    def test_invalid_syntax_triple_colon_project_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt2:alt:dataset.table',
                                  'project', 'var_x')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")

    def test_invalid_syntax_triple_dot_var(self):
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
        self.assertIn("JSON", str(context.exception))

# Helpers to test_cancel_queries that have mock_poll_job_complete returning false, unless mock_job_cancel was called with the same job_id
mock_canceled_jobs = []
def mock_poll_job_complete(job_id):
    return job_id in mock_canceled_jobs

def mock_job_cancel(projectId, jobId):
    mock_canceled_jobs.append(jobId)
    return mock.Mock()

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
    
    @mock.patch("airflow.contrib.hooks.bigquery_hook.LoggingMixin")
    @mock.patch("airflow.contrib.hooks.bigquery_hook.time")
    def test_cancel_queries(self, mocked_logging, mocked_time):
        project_id = 12345
        running_job_id = 3
        
        mock_jobs = mock.Mock()
        mock_jobs.cancel = mock.Mock(side_effect=mock_job_cancel)
        mock_service = mock.Mock()
        mock_service.jobs = mock.Mock(return_value=mock_jobs)
        
        bq_hook = hook.BigQueryBaseCursor(mock_service, project_id)
        bq_hook.running_job_id = running_job_id
        bq_hook.poll_job_complete = mock.Mock(side_effect=mock_poll_job_complete)
        
        bq_hook.cancel_query()
        
        mock_jobs.cancel.assert_called_with(projectId=project_id, jobId=running_job_id)

if __name__ == '__main__':
    unittest.main()

