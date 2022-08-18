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

import multiprocessing

from airflow.configuration import conf


class MultiprocessingStartMethodMixin:
    """Convenience class to add support for different types of multiprocessing."""

    def _get_multiprocessing_start_method(self) -> str:
        """
        Determine method of creating new processes by checking if the
        mp_start_method is set in configs, else, it uses the OS default.
        """
        if conf.has_option('core', 'mp_start_method'):
            return conf.get_mandatory_value('core', 'mp_start_method')

        method = multiprocessing.get_start_method()
        if not method:
            raise ValueError("Failed to determine start method")
        return method
