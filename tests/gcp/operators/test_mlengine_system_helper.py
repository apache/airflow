#!/usr/bin/env python
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
from urllib.parse import urlparse

from airflow.gcp.example_dags.example_mlengine import (
    JOB_DIR, PREDICTION_OUTPUT, SAVED_MODEL_PATH, SUMMARY_STAGING, SUMMARY_TMP,
)
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor


class MlEngineSystemTestHelper(LoggingCommandExecutor):
    def create_gcs_buckets(self):
        for bucket_name in self.get_ephemeral_bucket_names():
            self.execute_cmd(["gsutil", "mb", "gs://{bucket}".format(bucket=bucket_name)])

    def delete_gcs_buckets(self):
        for bucket_name in self.get_ephemeral_bucket_names():
            self.execute_cmd(["gsutil", "rm", "-r", "gs://{bucket}".format(bucket=bucket_name)])

    @staticmethod
    def get_ephemeral_bucket_names():
        return {
            urlparse(bucket_url).netloc
            for bucket_url in {SAVED_MODEL_PATH, JOB_DIR, PREDICTION_OUTPUT, SUMMARY_TMP, SUMMARY_STAGING}
        }
