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
import os
try:
    from contextdecorator import ContextDecorator
except ImportError:
    from contextlib import ContextDecorator

from shutil import move
from tempfile import mkdtemp
from unittest import SkipTest, TestCase

from airflow import AirflowException, models, settings
from airflow.configuration import AIRFLOW_HOME, AirflowConfigParser, get_airflow_config
from airflow.utils import db
from airflow.utils.log.logging_mixin import LoggingMixin

DEFAULT_DAG_FOLDER = "example_dags"

SKIP_SYSTEM_TEST_WARNING = """Skipping system test.
To allow system test set ENABLE_SYSTEM_TESTS=true.
"""


def resolve_dags_folder():
    """
    Returns DAG folder specified in current Airflow config.
    """
    config_file = get_airflow_config(AIRFLOW_HOME)
    conf = AirflowConfigParser()
    conf.read(config_file)
    try:
        dags = conf.get("core", "dags_folder")
    except AirflowException:
        dags = os.path.join(AIRFLOW_HOME, 'dags')
    return dags


class empty_dags_directory(  # pylint:disable=invalid-name
    ContextDecorator, LoggingMixin
):
    """
    Context manager that temporally removes DAGs from provided directory.
    """

    def __init__(self, dag_directory):
        super(empty_dags_directory, self).__init__()
        self.dag_directory = dag_directory
        self.temp_dir = mkdtemp()

    def __enter__(self):
        self._store_dags_to_temporary_directory(self.dag_directory, self.temp_dir)
        return self.temp_dir

    def __exit__(self, *args, **kwargs):
        self._restore_dags_from_temporary_directory(self.dag_directory, self.temp_dir)

    def _store_dags_to_temporary_directory(
        self, dag_folder, temp_dir
    ):
        self.log.info(
            "Storing DAGS from %s to temporary directory %s", dag_folder, temp_dir
        )
        try:
            os.mkdir(dag_folder)
        except OSError:
            pass
        for file in os.listdir(dag_folder):
            move(os.path.join(dag_folder, file), os.path.join(temp_dir, file))

    def _restore_dags_from_temporary_directory(
        self, dag_folder, temp_dir
    ):
        self.log.info(
            "Restoring DAGS to %s from temporary directory %s", dag_folder, temp_dir
        )
        for file in os.listdir(temp_dir):
            move(os.path.join(temp_dir, file), os.path.join(dag_folder, file))


class SystemTest(TestCase, LoggingMixin):
    def run(self, result=None):
        if os.environ.get('ENABLE_SYSTEM_TESTS') != 'true':
            raise SkipTest(SKIP_SYSTEM_TEST_WARNING)
        return super(SystemTest, self).run(result)

    def setUp(self):
        """
        We want to avoid random errors while database got reset - those
        Are apparently triggered by parser trying to parse DAGs while
        The tables are dropped. We move the dags temporarily out of the dags folder
        and move them back after reset
        """
        dag_folder = resolve_dags_folder()
        with empty_dags_directory(dag_folder):
            db.resetdb(settings.RBAC)
        super(SystemTest, self).setUp()

    @staticmethod
    def _create_path_to_dags(path):
        """
        Creates relative path to DAGs folder `{$AIRFLOWHOME}/path`.
        Accepts both formats `my/dags/folder` and `my.dags.folder`.
        """
        path = path.replace(".", "/")
        return os.path.join(AIRFLOW_HOME, path)

    def run_dag(self, dag_id, dag_folder=DEFAULT_DAG_FOLDER):
        """
        Runs example dag by it's ID.

        :param dag_id: id of a DAG to be run
        :type dag_id: str
        :param dag_folder: directory where to look for the specific DAG. Relative to AIRFLOW_HOME.
        :type dag_folder: str
        """
        dag_folder_path = self._create_path_to_dags(dag_folder)

        self.log.info("Looking for DAG: %s in %s", dag_id, dag_folder_path)
        dag_bag = models.DagBag(dag_folder=dag_folder_path, include_examples=False)
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {dag_id} could not be found. It's either an import problem,"
                "wrong dag_id or DAG is not in provided dag_folder."
                "The content of the {dag_folder} folder is {content}".format(
                    dag_id=dag_id,
                    dag_folder=dag_folder_path,
                    content=os.listdir(dag_folder_path),
                )
            )

        self.log.info("Attempting to run DAG: %s", dag_id)
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)
