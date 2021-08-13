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
from unittest import TestCase
from tests.compat import mock

from airflow.upgrade.rules.task_handlers_moved import TaskHandlersMovedRule
from tests.test_utils.config import conf_vars

OLD_PATH = 'airflow.utils.log.gcs_task_handler.GCSTaskHandler'
NEW_PATH = "airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler"

BAD_LOG_CONFIG_DICT = {
    'handlers': {
        'task': {
            'class': OLD_PATH,
        },
    }}

GOOD_LOG_CONFIG_DICT = {
    'handlers': {
        'task': {
            'class': NEW_PATH,
        },
    }}


class TestTaskHandlersMovedRule(TestCase):

    @conf_vars({("core", "logging_config_class"): "dummy_log.conf"})
    @mock.patch("airflow.upgrade.rules.task_handlers_moved.import_string")
    def test_invalid_check(self, mock_import_str):
        mock_import_str.return_value = BAD_LOG_CONFIG_DICT
        rule = TaskHandlersMovedRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        msg = "This path : `{old}` should be updated to this path: `{new}`".format(old=OLD_PATH,
                                                                                   new=NEW_PATH)
        response = rule.check()
        assert response == [msg]

    @conf_vars({("core", "logging_config_class"): "dummy_log.conf"})
    @mock.patch("airflow.upgrade.rules.task_handlers_moved.import_string")
    def test_valid_check(self, mock_import_str):
        mock_import_str.return_value = GOOD_LOG_CONFIG_DICT
        rule = TaskHandlersMovedRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        msg = None
        response = rule.check()
        assert response == msg
