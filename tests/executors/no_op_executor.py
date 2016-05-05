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

from airflow.executors.base_executor import BaseExecutor


class NoOpExecutor(BaseExecutor):
    """
    This executor does not run any tasks and is used for testing only.
    """
    def execute_async(self, key, command, queue=None):
        self.logger.info("Tried to execute {}".format(command))
        pass

    def sync(self):
        pass

    def end(self):
        pass

    def terminate(self):
        pass

    def start(self):
        pass
