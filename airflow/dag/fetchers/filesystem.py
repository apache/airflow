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

from datetime import datetime

import sys
import os
import re
import zipfile
import hashlib
import imp
import importlib

import airflow
from airflow import configuration
from airflow.utils import timezone
from airflow.utils.timeout import timeout
from airflow.exceptions import AirflowDagCycleException
from airflow.dag.fetchers.base import BaseDagFetcher


class FileSystemDagFetcher(BaseDagFetcher):
    """
    Fetches dags from the local file system, by walking the dags_uri
    folder on the local disk, looking for .py and .zip files.

    :param dagbag: a DagBag instance, which we will populate
    :type dagbag: DagBag
    :param dags_uri: the URI for the dags folder. The schema
        prefix determines the child that will be instantiated
    :type dags_uri: string
    :param safe_mode: if dag files should be processed with safe_mode
    :type safe_mode: boolean
    """
    def process_file(self, filepath, only_if_updated=True):
        """
        Given a path to a python module or zip file, this method imports
        the module and look for dag objects within it.
        """
        found_dags = []
        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?
        if filepath is None or not os.path.isfile(filepath):
            return found_dags

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed = datetime.fromtimestamp(
                os.path.getmtime(filepath))
            if only_if_updated \
                    and filepath in self.dagbag.file_last_changed \
                    and file_last_changed == self.dagbag.file_last_changed[filepath]:
                return found_dags

        except Exception as e:
            self.log.exception(e)
            return found_dags

        mods = []
        if not zipfile.is_zipfile(filepath):
            if self.safe_mode and os.path.isfile(filepath):
                with open(filepath, 'rb') as f:
                    content = f.read()
                    if not all([s in content for s in (b'DAG', b'airflow')]):
                        self.dagbag.file_last_changed[filepath] = file_last_changed
                        return found_dags

            self.log.debug("Importing %s", filepath)
            org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
            mod_name = ('unusual_prefix_' +
                        hashlib.sha1(filepath.encode('utf-8')).hexdigest() +
                        '_' + org_mod_name)

            if mod_name in sys.modules:
                del sys.modules[mod_name]

            with timeout(configuration.getint('core', "DAGBAG_IMPORT_TIMEOUT")):
                try:
                    m = imp.load_source(mod_name, filepath)
                    mods.append(m)
                except Exception as e:
                    self.log.exception("Failed to import: %s", filepath)
                    self.dagbag.import_errors[filepath] = str(e)
                    self.dagbag.file_last_changed[filepath] = file_last_changed

        else:
            zip_file = zipfile.ZipFile(filepath)
            for mod in zip_file.infolist():
                head, _ = os.path.split(mod.filename)
                mod_name, ext = os.path.splitext(mod.filename)
                if not head and (ext == '.py' or ext == '.pyc'):
                    if mod_name == '__init__':
                        self.log.warning("Found __init__.%s at root of %s", ext, filepath)
                    if self.safe_mode:
                        with zip_file.open(mod.filename) as zf:
                            self.log.debug("Reading %s from %s", mod.filename, filepath)
                            content = zf.read()
                            if not all([s in content for s in (b'DAG', b'airflow')]):
                                self.dagbag.file_last_changed[filepath] = (
                                    file_last_changed)
                                # todo: create ignore list
                                return found_dags

                    if mod_name in sys.modules:
                        del sys.modules[mod_name]

                    try:
                        sys.path.insert(0, filepath)
                        m = importlib.import_module(mod_name)
                        mods.append(m)
                    except Exception as e:
                        self.log.exception("Failed to import: %s", filepath)
                        self.dagbag.import_errors[filepath] = str(e)
                        self.dagbag.file_last_changed[filepath] = file_last_changed

        for m in mods:
            for dag in list(m.__dict__.values()):
                if isinstance(dag, airflow.models.DAG):
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                        if dag.fileloc != filepath:
                            dag.fileloc = filepath
                    try:
                        dag.is_subdag = False
                        self.dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
                        found_dags.append(dag)
                        found_dags += dag.subdags
                    except AirflowDagCycleException as cycle_exception:
                        self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                        self.dagbag.import_errors[dag.full_filepath] = \
                            str(cycle_exception)
                        self.dagbag.file_last_changed[dag.full_filepath] = \
                            file_last_changed

        self.dagbag.file_last_changed[filepath] = file_last_changed
        return found_dags

    def fetch(self, only_if_updated=True):
        """
        Walks the dags_folder (self.dags_uri) looking for files to process
        """
        if os.path.isfile(self.dags_uri):
            self.process_file(self.dags_uri, only_if_updated=only_if_updated)
        elif os.path.isdir(self.dags_uri):
            patterns = []
            for root, dirs, files in os.walk(self.dags_uri, followlinks=True):
                ignore_file = [f for f in files if f == '.airflowignore']
                if ignore_file:
                    f = open(os.path.join(root, ignore_file[0]), 'r')
                    patterns += [p for p in f.read().split('\n') if p]
                    f.close()
                for f in files:
                    try:
                        filepath = os.path.join(root, f)
                        if not os.path.isfile(filepath):
                            continue
                        mod_name, file_ext = os.path.splitext(
                            os.path.split(filepath)[-1])
                        if file_ext != '.py' and not zipfile.is_zipfile(filepath):
                            continue
                        if not any(
                                [re.findall(p, filepath) for p in patterns]):
                            ts = timezone.utcnow()
                            found_dags = self.process_file(
                                filepath, only_if_updated=only_if_updated)

                            td = timezone.utcnow() - ts
                            td = td.total_seconds() + (
                                float(td.microseconds) / 1000000)
                            self.stats.append(self.FileLoadStat(
                                filepath.replace(self.dags_uri, ''),
                                td,
                                len(found_dags),
                                sum([len(dag.tasks) for dag in found_dags]),
                                str([dag.dag_id for dag in found_dags]),
                            ))
                    except Exception as e:
                        self.log.exception(e)

        return self.stats
