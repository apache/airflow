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

import base64
import binascii
import unittest
from unittest import mock

import dill

try:
    from airflow.providers.google.cloud.utils import mlengine_prediction_summary
except ImportError as e:
    if 'apache_beam' in str(e):
        raise unittest.SkipTest(f"package apache_beam not present. Skipping all tests in {__name__}")


class TestJsonCode(unittest.TestCase):
    def test_encode(self):
        self.assertEqual(b'{"a": 1}', mlengine_prediction_summary.JsonCoder.encode({'a': 1}))

    def test_decode(self):
        self.assertEqual({'a': 1}, mlengine_prediction_summary.JsonCoder.decode('{"a": 1}'))


class TestMakeSummary(unittest.TestCase):
    def test_make_summary(self):
        print(mlengine_prediction_summary.MakeSummary(1, lambda x: x, []))

    def test_run_without_all_arguments_should_raise_exception(self):
        with self.assertRaises(SystemExit):
            mlengine_prediction_summary.run()

        with self.assertRaises(SystemExit):
            mlengine_prediction_summary.run(
                [
                    "--prediction_path=some/path",
                ]
            )

        with self.assertRaises(SystemExit):
            mlengine_prediction_summary.run(
                [
                    "--prediction_path=some/path",
                    "--metric_fn_encoded=encoded_text",
                ]
            )

    def test_run_should_fail_for_invalid_encoded_fn(self):
        with self.assertRaises(binascii.Error):
            mlengine_prediction_summary.run(
                [
                    "--prediction_path=some/path",
                    "--metric_fn_encoded=invalid_encoded_text",
                    "--metric_keys=a",
                ]
            )

    def test_run_should_fail_if_enc_fn_is_not_callable(self):
        non_callable_value = 1
        fn_enc = base64.b64encode(dill.dumps(non_callable_value)).decode('utf-8')

        with self.assertRaises(ValueError):
            mlengine_prediction_summary.run(
                [
                    "--prediction_path=some/path",
                    "--metric_fn_encoded=" + fn_enc,
                    "--metric_keys=a",
                ]
            )

    @mock.patch.object(mlengine_prediction_summary.beam.pipeline, "PipelineOptions")
    @mock.patch.object(mlengine_prediction_summary.beam, "Pipeline")
    @mock.patch.object(mlengine_prediction_summary.beam.io, "ReadFromText")
    def test_run_should_not_fail_with_valid_fn(self, io_mock, pipeline_obj_mock, pipeline_mock):
        def metric_function():
            return 1

        fn_enc = base64.b64encode(dill.dumps(metric_function)).decode('utf-8')

        mlengine_prediction_summary.run(
            [
                "--prediction_path=some/path",
                "--metric_fn_encoded=" + fn_enc,
                "--metric_keys=a",
            ]
        )

        pipeline_mock.assert_called_once_with([])
        pipeline_obj_mock.assert_called_once()
        io_mock.assert_called_once()
