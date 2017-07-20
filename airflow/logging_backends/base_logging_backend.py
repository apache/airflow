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


class BaseLoggingBackend(object):
    def __init__(self, **kwargs):
        self.schema_name = kwargs.get('schema_name')

    def get_logs(self, dag_id, task_id, execution_date, **kwargs):
        """
        Get logs given dag_id, task_id and execution_date.
        Return a list of Log obejct.
        """
        raise NotImplementedError()

class Log(object):
    """
    An abstract class for single line of Airflow log.
                       whole line is message
                                v
    [2017-06-09 13:14:24,965] {cli.py:389} INFO - Running on host
                ^                   ^        ^           ^
            [timestamp]    {filename:fileline} log_level - details
    """
    def __init__(self, log):
        self.log = log

    @property
    def message(self):
        raise NotImplementedError()

    @property
    def timestamp(self):
        raise NotImplementedError()

    @property
    def filename(self):
        raise NotImplementedError()

    @property
    def fileline(self):
        raise NotImplementedError()

    @property
    def log_level(self):
        raise NotImplementedError()

    @property
    def details(self):
        raise NotImplementedError()
