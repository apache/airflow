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

import collections
import json
import unittest

from airflow import configuration, models
from airflow.contrib.hooks.sqoop_hook import SqoopHook
from airflow.exceptions import AirflowException
from airflow.utils import db

from mock import patch, call

from io import StringIO


class TestSqoopHook(unittest.TestCase):
    _config = {
        'conn_id': 'sqoop_test',
        'num_mappers': 22,
        'verbose': True,
        'properties': {
            'mapred.map.max.attempts': '1'
        },
        'hcatalog_database': 'hive_database',
        'hcatalog_table': 'hive_table'
    }
    _config_export = {
        'table': 'domino.export_data_to',
        'export_dir': '/hdfs/data/to/be/exported',
        'input_null_string': '\\n',
        'input_null_non_string': '\\t',
        'staging_table': 'database.staging',
        'clear_staging_table': True,
        'enclosed_by': '"',
        'escaped_by': '\\',
        'input_fields_terminated_by': '|',
        'input_lines_terminated_by': '\n',
        'input_optionally_enclosed_by': '"',
        'batch': True,
        'relaxed_isolation': True,
        'extra_export_options': collections.OrderedDict([
            ('update-key', 'id'),
            ('update-mode', 'allowinsert')
        ])
    }
    _config_import = {
        'target_dir': '/hdfs/data/target/location',
        'append': True,
        'file_type': 'parquet',
        'split_by': '\n',
        'direct': True,
        'driver': 'com.microsoft.jdbc.sqlserver.SQLServerDriver',
        'extra_import_options': {
            'hcatalog-storage-stanza': "\"stored as orcfile\"",
            'show': ''
        }
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
                conn_id='sqoop_test', conn_type='sqoop', schema='schema',
                host='rmdbs', port=5050, extra=json.dumps(self._config_json)
            )
        )

    @patch('subprocess.Popen')
    def test_popen(self, mock_popen):
        # Given
        mock_popen.return_value.stdout = StringIO(u'stdout')
        mock_popen.return_value.stderr = StringIO(u'stderr')
        mock_popen.return_value.returncode = 0
        mock_popen.return_value.communicate.return_value = \
            [StringIO(u'stdout\nstdout'), StringIO(u'stderr\nstderr')]

        # When
        hook = SqoopHook(conn_id='sqoop_test')
        hook.export_table(**self._config_export)

        # Then
        self.assertEqual(mock_popen.mock_calls[0], call(
            ['sqoop',
             'export',
             '-fs', self._config_json['namenode'],
             '-jt', self._config_json['job_tracker'],
             '-libjars', self._config_json['libjars'],
             '-files', self._config_json['files'],
             '-archives', self._config_json['archives'],
             '--connect', 'rmdbs:5050/schema',
             '--input-null-string', self._config_export['input_null_string'],
             '--input-null-non-string', self._config_export['input_null_non_string'],
             '--staging-table', self._config_export['staging_table'],
             '--clear-staging-table',
             '--enclosed-by', self._config_export['enclosed_by'],
             '--escaped-by', self._config_export['escaped_by'],
             '--input-fields-terminated-by', self._config_export['input_fields_terminated_by'],
             '--input-lines-terminated-by', self._config_export['input_lines_terminated_by'],
             '--input-optionally-enclosed-by', self._config_export['input_optionally_enclosed_by'],
             '--batch',
             '--relaxed-isolation',
             '--export-dir', self._config_export['export_dir'],
             '--update-key', 'id',
             '--update-mode', 'allowinsert',
             '--table', self._config_export['table']], stderr=-2, stdout=-1))

    def test_submit_none_mappers(self):
        """
        Test to check that if value of num_mappers is None, then it shouldn't be in the cmd built.
        """
        _config_without_mappers = self._config.copy()
        _config_without_mappers['num_mappers'] = None

        hook = SqoopHook(**_config_without_mappers)
        cmd = ' '.join(hook._prepare_command())
        self.assertNotIn('--num-mappers', cmd)

    def test_submit(self):
        """
        Tests to verify that from connection extra option the options are added to the Sqoop command.
        """
        hook = SqoopHook(**self._config)

        cmd = ' '.join(hook._prepare_command())

        # Check if the config has been extracted from the json
        if self._config_json['namenode']:
            self.assertIn("-fs {}".format(self._config_json['namenode']), cmd)

        if self._config_json['job_tracker']:
            self.assertIn("-jt {}".format(self._config_json['job_tracker']), cmd)

        if self._config_json['libjars']:
            self.assertIn("-libjars {}".format(self._config_json['libjars']), cmd)

        if self._config_json['files']:
            self.assertIn("-files {}".format(self._config_json['files']), cmd)

        if self._config_json['archives']:
            self.assertIn("-archives {}".format(self._config_json['archives']), cmd)

        self.assertIn("--hcatalog-database {}".format(self._config['hcatalog_database']), cmd)
        self.assertIn("--hcatalog-table {}".format(self._config['hcatalog_table']), cmd)

        # Check the regulator stuff passed by the default constructor
        if self._config['verbose']:
            self.assertIn("--verbose", cmd)

        if self._config['num_mappers']:
            self.assertIn("--num-mappers {}".format(self._config['num_mappers']), cmd)

        for key, value in self._config['properties'].items():
            self.assertIn("-D {}={}".format(key, value), cmd)

        # We don't have the sqoop binary available, and this is hard to mock,
        # so just accept an exception for now.
        with self.assertRaises(OSError):
            hook.export_table(**self._config_export)

        with self.assertRaises(OSError):
            hook.import_table(table='schema.table', target_dir='/sqoop/example/path')

        with self.assertRaises(OSError):
            hook.import_query(query='SELECT * FROM sometable', target_dir='/sqoop/example/path')

    def test_export_cmd(self):
        """
        Tests to verify the hook export command is building correct Sqoop export command.
        """
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
                relaxed_isolation=self._config_export['relaxed_isolation'],
                extra_export_options=self._config_export['extra_export_options']
            )
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
        # these options are from the extra export options
        self.assertIn("--update-key id", cmd)
        self.assertIn("--update-mode allowinsert", cmd)

        if self._config_export['clear_staging_table']:
            self.assertIn("--clear-staging-table", cmd)

        if self._config_export['batch']:
            self.assertIn("--batch", cmd)

        if self._config_export['relaxed_isolation']:
            self.assertIn("--relaxed-isolation", cmd)

    def test_import_cmd(self):
        """
        Tests to verify the hook import command is building correct Sqoop import command.
        """
        hook = SqoopHook()

        # The subprocess requires an array but we build the cmd by joining on a space
        cmd = ' '.join(
            hook._import_cmd(
                self._config_import['target_dir'],
                append=self._config_import['append'],
                file_type=self._config_import['file_type'],
                split_by=self._config_import['split_by'],
                direct=self._config_import['direct'],
                driver=self._config_import['driver'],
                extra_import_options=None
            )
        )

        if self._config_import['append']:
            self.assertIn('--append', cmd)

        if self._config_import['direct']:
            self.assertIn('--direct', cmd)

        self.assertIn('--target-dir {}'.format(
            self._config_import['target_dir']), cmd)

        self.assertIn('--driver {}'.format(self._config_import['driver']), cmd)
        self.assertIn('--split-by {}'.format(self._config_import['split_by']), cmd)
        # these are from extra options, but not passed to this cmd import command
        self.assertNotIn('--show', cmd)
        self.assertNotIn('hcatalog-storage-stanza \"stored as orcfile\"', cmd)

        cmd = ' '.join(
            hook._import_cmd(
                target_dir=None,
                append=self._config_import['append'],
                file_type=self._config_import['file_type'],
                split_by=self._config_import['split_by'],
                direct=self._config_import['direct'],
                driver=self._config_import['driver'],
                extra_import_options=self._config_import['extra_import_options']
            )
        )

        self.assertNotIn('--target-dir', cmd)
        # these checks are from the extra import options
        self.assertIn('--show', cmd)
        self.assertIn('hcatalog-storage-stanza \"stored as orcfile\"', cmd)

    def test_get_export_format_argument(self):
        """
        Tests to verify the hook get format function is building
        correct Sqoop command with correct format type.
        """
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

    def test_cmd_mask_password(self):
        """
        Tests to verify the hook masking function will correctly mask a user password in Sqoop command.
        """
        hook = SqoopHook()
        self.assertEqual(
            hook.cmd_mask_password(['--password', 'supersecret']),
            ['--password', 'MASKED']
        )

        cmd = ['--target', 'targettable']
        self.assertEqual(
            hook.cmd_mask_password(cmd),
            cmd
        )


if __name__ == '__main__':
    unittest.main()
