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

from contextlib import ContextDecorator

from unittest.mock import Mock

from airflow import conf


# So we don't depend on actual value in the test config.
class mock_conf_get(ContextDecorator):
    def __init__(self, mock_section, mock_key, mock_return_value):
        self.mock_section = mock_section
        self.mock_key = mock_key
        self.mock_return_value = mock_return_value

    def __enter__(self):
        self.old_conf_get = conf.getint

        def side_effect(section, key):
            if section == self.mock_section and key == self.mock_key:
                return self.mock_return_value
            else:
                return self.old_conf_get(section, key)

        conf.getint = Mock(side_effect=side_effect)
        return self

    def __exit__(self, *exc):
        conf.getint = self.old_conf_get
        return False
