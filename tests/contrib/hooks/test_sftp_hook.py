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

import importlib
from unittest import TestCase

OLD_PATH = "airflow.contrib.hooks.sftp_hook"
NEW_PATH = "airflow.providers.sftp.hooks.sftp_hook"
WARNING_MESSAGE = "This module is deprecated. Please use `{}`.".format(NEW_PATH)


class TestMovingSFTPHookToCore(TestCase):
    def test_move_sftp_hook_to_core(self):
        with self.assertWarns(DeprecationWarning) as warn:
            # Reload to see deprecation warning each time
            importlib.import_module(OLD_PATH)
        self.assertEqual(WARNING_MESSAGE, str(warn.warning))
