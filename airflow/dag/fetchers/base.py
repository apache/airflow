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

from collections import namedtuple

from airflow.utils.log.logging_mixin import LoggingMixin


class BaseDagFetcher(LoggingMixin):
    """
    Abstract base class for all DagFetchers.

    A DagFetcher's responsability is to find the dags in
    the dags_uri and add them to the dagbag.

    The fetch method must be implemented by any given DagFetcher,
    and return the list of per dag statistics. It must also
    implement a process_file method, which is used to reprocess
    a DAG.

    :param dagbag: a DagBag instance, which we will populate
    :type dagbag: DagBag
    :param dags_uri: the URI for the dags folder. The schema
        prefix determines the child that will be instantiated
    :type dags_uri: string
    :param safe_mode: if dag files should be processed with safe_mode
    :type safe_mode: boolean
    """
    FileLoadStat = namedtuple(
        'FileLoadStat', 'file duration dag_num task_num dags')

    def __init__(self, dagbag, dags_uri=None, safe_mode=True):
        self.found_dags = []
        self.stats = []
        self.dagbag = dagbag
        self.dags_uri = dags_uri
        self.safe_mode = safe_mode

    def process_file(self, filepath, only_if_updated=True):
        """
        This method is used to process/reprocess a single file and
        must be implemented by all DagFetchers.

        Must return the dags in the file.
        """
        raise NotImplementedError()

    def fetch(self, only_if_updated=True):
        """
        This is the main method to derive when creating a DagFetcher.
        """
        raise NotImplementedError()
