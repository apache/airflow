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

import json
import unittest

from airflow import configuration, models
from airflow.contrib.hooks.sqoop_hook import SqoopHook
from airflow.exceptions import AirflowException
from airflow.utils import db


class TestSqoopHook(unittest.TestCase):
    _config = {
        'conn_id': 'sqoop_test',
        'num_mappers': 22,
        'verbose': True,
        'properties': {
            'mapred.map.max.attempts': '1'
        }
    }
    _config_export = {
        'table': 'domino.export_data_to',
        'export_dir': '/hdfs/data/to/be/exported',
        'input_null_string': '\n',
        'input_null_non_string': '\t',
        'staging_table': 'database.staging',
        'clear_staging_table': True,
        'enclosed_by': '"',
        'escaped_by': '\\',
        'input_fields_terminated_by': '|',
        'input_lines_terminated_by': '\n',
        'input_optionally_enclosed_by': '"',
        'batch': True,
        'relaxed_isolation': True
    }
    _config_import = {
        'target_dir': '/hdfs/data/target/location',
        'append': True,
        'file_type': 'parquet',
        'split_by': '\n',
        'direct': True,
        'driver': 'com.microsoft.jdbc.sqlserver.SQLServerDriver'
    }

    _config_json = {
        'namenode': 'http://0.0.0.0:50070/',
        'job_tracker': 'http://0.0.0.0:50030/',
        'libjars': '/path/to/jars',
        'files': '/path/to/files',
        'archives': '/path/to/archives'
    }

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='sqoop_test', conn_type='sqoop',
                host='rmdbs', port=5050, extra=json.dumps(self._config_json)
            )
        )

    def test_popen(self):
        hook = SqoopHook(**self._config)

        # Should go well
        hook.Popen(['ls'])

        # Should give an exception
        with self.assertRaises(OSError):
            hook.Popen('exit 1')

    def test_submit(self):
        hook = SqoopHook(**self._config)

        cmd = ' '.join(hook._prepare_command())

        # Check if the config has been extracted from the json
        if self._config_json['namenode']:
            self.assertIn("-fs {}".format(self._config_json['namenode']), cmd)

        if self._config_json['job_tracker']:
            self.assertIn("-jt {}".format(self._config_json['job_tracker']),
                          cmd)

        if self._config_json['libjars']:
            self.assertIn("-libjars {}".format(self._config_json['libjars']),
                          cmd)

        if self._config_json['files']:
            self.assertIn("-files {}".format(self._config_json['files']), cmd)

        if self._config_json['archives']:
            self.assertIn(
                "-archives {}".format(self._config_json['archives']), cmd
            )

        # Check the regulator stuff passed by the default constructor
        if self._config['verbose']:
            self.assertIn("--verbose", cmd)

        if self._config['num_mappers']:
            self.assertIn(
                "--num-mappers {}".format(self._config['num_mappers']), cmd
            )

        print(self._config['properties'])
        for key, value in self._config['properties'].items():
            self.assertIn("-D {}={}".format(key, value), cmd)

        # We don't have the sqoop binary available, and this is hard to mock,
        # so just accept an exception for now.
        with self.assertRaises(OSError):
            hook.export_table(**self._config_export)

        with self.assertRaises(OSError):
            hook.import_table(table='schema.table',
                              target_dir='/sqoop/example/path')

        with self.assertRaises(OSError):
            hook.import_query(query='SELECT * FROM sometable',
                              target_dir='/sqoop/example/path')

    def test_export_cmd(self):
        hook = SqoopHook()

        # The subprocess requires an array but we build the cmd by joining on a space
        cmd = ' '.join(
            hook._export_cmd(
                self._config_export['table'],
                self._config_export['export_dir'],
                input_null_string=self._config_export['input_null_string'],
                input_null_non_string=self._config_export[
                    'input_null_non_string'],
                staging_table=self._config_export['staging_table'],
                clear_staging_table=self._config_export['clear_staging_table'],
                enclosed_by=self._config_export['enclosed_by'],
                escaped_by=self._config_export['escaped_by'],
                input_fields_terminated_by=self._config_export[
                    'input_fields_terminated_by'],
                input_lines_terminated_by=self._config_export[
                    'input_lines_terminated_by'],
                input_optionally_enclosed_by=self._config_export[
                    'input_optionally_enclosed_by'],
                batch=self._config_export['batch'],
                relaxed_isolation=self._config_export['relaxed_isolation'])
        )

        self.assertIn("--input-null-string {}".format(
            self._config_export['input_null_string']), cmd)
        self.assertIn("--input-null-non-string {}".format(
            self._config_export['input_null_non_string']), cmd)
        self.assertIn("--staging-table {}".format(
            self._config_export['staging_table']), cmd)
        self.assertIn("--enclosed-by {}".format(
            self._config_export['enclosed_by']), cmd)
        self.assertIn("--escaped-by {}".format(
            self._config_export['escaped_by']), cmd)
        self.assertIn("--input-fields-terminated-by {}".format(
            self._config_export['input_fields_terminated_by']), cmd)
        self.assertIn("--input-lines-terminated-by {}".format(
            self._config_export['input_lines_terminated_by']), cmd)
        self.assertIn("--input-optionally-enclosed-by {}".format(
            self._config_export['input_optionally_enclosed_by']), cmd)

        if self._config_export['clear_staging_table']:
            self.assertIn("--clear-staging-table", cmd)

        if self._config_export['batch']:
            self.assertIn("--batch", cmd)

        if self._config_export['relaxed_isolation']:
            self.assertIn("--relaxed-isolation", cmd)

    def test_import_cmd(self):
        hook = SqoopHook()

        # The subprocess requires an array but we build the cmd by joining on a space
        cmd = ' '.join(
            hook._import_cmd(self._config_import['target_dir'],
                             append=self._config_import['append'],
                             file_type=self._config_import['file_type'],
                             split_by=self._config_import['split_by'],
                             direct=self._config_import['direct'],
                             driver=self._config_import['driver'])
        )

        if self._config_import['append']:
            self.assertIn('--append', cmd)

        if self._config_import['direct']:
            self.assertIn('--direct', cmd)

        self.assertIn('--target-dir {}'.format(
            self._config_import['target_dir']), cmd)

        self.assertIn('--driver {}'.format(self._config_import['driver']), cmd)
        self.assertIn('--split-by {}'.format(self._config_import['split_by']),
                      cmd)

    def test_get_export_format_argument(self):
        hook = SqoopHook()
        self.assertIn("--as-avrodatafile",
                      hook._get_export_format_argument('avro'))
        self.assertIn("--as-parquetfile",
                      hook._get_export_format_argument('parquet'))
        self.assertIn("--as-sequencefile",
                      hook._get_export_format_argument('sequence'))
        self.assertIn("--as-textfile",
                      hook._get_export_format_argument('text'))
        with self.assertRaises(AirflowException):
            hook._get_export_format_argument('unknown')


if __name__ == '__main__':
    unittest.main()
