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

from __future__ import absolute_import

import os
import re
import logging
import warnings
from airflow.utils.dag_processing import correct_maybe_zipped, list_py_file_paths
from airflow import conf
from airflow.models.dagbag import DagBag
from airflow.upgrade.rules.base_rule import BaseRule


class NoAdditionalArgsInOperatorsRule(BaseRule):
    title = "No additional argument allowed in BaseOperator."

    description = """\
Passing unrecognized arguments to operators is not allowed in Airflow 2.0 anymore,
and will cause an exception.
                  """

    def check(self, dags_folder=None):
        if not dags_folder:
            dags_folder = conf.get("core", "dags_folder")

        logger = logging.root
        old_level = logger.level
        try:
            logger.setLevel(logging.ERROR)
            dagbag = DagBag(dag_folder=os.devnull, include_examples=False, store_serialized_dags=False)
            dags_folder = correct_maybe_zipped(dags_folder)

            # Each file in the DAG folder is parsed individually
            for filepath in list_py_file_paths(dags_folder,
                                               safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'),
                                               include_examples=False):
                try:
                    with warnings.catch_warnings(record=True) as captured_warnings:
                        _ = dagbag.process_file(
                            filepath,
                            safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'))
                except Exception:
                    pass

                for warning in captured_warnings:
                    if warning.category in (DeprecationWarning, PendingDeprecationWarning) \
                            and str(warning.message).startswith("Invalid arguments were passed"):
                        m = re.match(r'''
                                    .* \(task_id:\ ([^\)]+)\) .* \n
                                    \*args:\ (.*) \n
                                     \*\*kwargs:\ (.*)
                                    ''', str(warning.message), re.VERBOSE)

                        yield "DAG file `{}` with task_id `{}` has unrecognized positional args `{}`" \
                              "and keyword args " \
                              "`{}`".format(filepath, m.group(1), m.group(2), m.group(3))
        finally:
            logger.setLevel(old_level)
