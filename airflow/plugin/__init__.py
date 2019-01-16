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
from abc import ABCMeta, abstractmethod


class AirflowPluginDagSource(object):
    """
    Allow users to create sources for dags to be loaded from
    """
    __metaclass__ = ABCMeta

    name = None  # type: basestring

    def __init__(self, config=None):
        self._config = config

    @abstractmethod
    def add_dags_to_dagbag(self, dagbag, *args, **kwargs):
        """
        Provide a dagbag to inject instantiated DAG objects directly
        into. More advanced than writing to disk, but leaves a cleaner
        footprint, and (probably) faster than writing to disk and reading
        back.

        :param dagbag:
        :param args: If future arguments are passed in on call.
        :param kwargs: If future arguments are passed in on call.
        :return:
        """
        pass

    @abstractmethod
    def put_dags_on_disk(self, dag_path, *args, **kwargs):
        """
        Provide a path for the plugin to write dags to file.
        This is a simpler method more in line with the current
        architecture of Airflow dag loading than adding dags
        directly to the dag bag as in
        `AirflowPluginDagSource.add_dags_to_dagbag`.

        :param dag_path: A path on disk for this plugin to store dags in
            if it needs to.
        :param args: If future arguments are passed in on call.
        :param kwargs: If future arguments are passed in on call.
        :return:
        """
        pass
