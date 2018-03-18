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

import sys

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.dag.fetchers.filesystem import FileSystemDagFetcher
from airflow.dag.fetchers.hdfs import HDFSDagFetcher
from airflow.dag.fetchers.s3 import S3DagFetcher
from airflow.dag.fetchers.gcs import GCSDagFetcher
from airflow.dag.fetchers.git import GitDagFetcher


def get_dag_fetcher(dagbag, dags_uri):
    """
    Factory method that returns an instance of the right
    DagFetcher, based on the dags_uri prefix.

    Any prefix that does not match keys in the dag_fetchers
    dict (or no prefix at all) defaults to FileSystemDagFetcher.
    """
    log = LoggingMixin().log

    dag_fetchers = dict(
        hdfs=HDFSDagFetcher,
        s3=S3DagFetcher,
        gcs=GCSDagFetcher,
        git=GitDagFetcher)

    uri_schema = dags_uri.split(':')[0]

    if uri_schema not in dag_fetchers:
        log.debug('Defaulting to FileSystemDagFetcher')
        return FileSystemDagFetcher(dagbag, dags_uri)

    return dag_fetchers[uri_schema](dagbag, dags_uri)


def _integrate_plugins():
    """Integrate plugins to the context."""
    from airflow.plugins_manager import dag_fetchers_modules
    for dag_fetchers_module in dag_fetchers_modules:
        sys.modules[dag_fetchers_module.__name__] = dag_fetchers_module
        globals()[dag_fetchers_module._name] = dag_fetchers_module
