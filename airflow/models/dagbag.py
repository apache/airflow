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

import hashlib
import importlib
import importlib.util
import os
import sys
import textwrap
import zipfile
from datetime import datetime, timedelta
from typing import NamedTuple

from croniter import CroniterBadCronError, CroniterBadDateError, CroniterNotAlphaError, croniter

from airflow import settings
from airflow.configuration import conf
from airflow.dag.base_dag import BaseDagBag
from airflow.exceptions import AirflowDagCycleException
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.file import correct_maybe_zipped
from airflow.utils.helpers import pprinttable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.timeout import timeout


class FileLoadStat(NamedTuple):
    """
    Information about single file
    """
    file: str
    duration: timedelta
    dag_num: int
    task_num: int
    dags: str


class DagBag(BaseDagBag, LoggingMixin):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high
    level configuration settings, like what database to use as a backend and
    what executor to use to fire off tasks. This makes it easier to run
    distinct environments for say production and development, tests, or for
    different teams or security profiles. What would have been system level
    settings are now dagbag level so that one system can run multiple,
    independent settings sets.

    :param dag_folder: the folder to scan to find DAGs
    :type dag_folder: unicode
    :param executor: the executor to use when executing task instances
        in this DagBag
    :param include_examples: whether to include the examples that ship
        with airflow or not
    :type include_examples: bool
    :param has_logged: an instance boolean that gets flipped from False to True after a
        file has been skipped. This is to prevent overloading the user with logging
        messages about skipped files. Therefore only once per DagBag is a file logged
        being skipped.
    :param store_serialized_dags: Read DAGs from DB if store_serialized_dags is ``True``.
        If ``False`` DAGs are read from python files.
    :type store_serialized_dags: bool
    """

    # static class variables to detetct dag cycle
    CYCLE_NEW = 0
    CYCLE_IN_PROGRESS = 1
    CYCLE_DONE = 2
    DAGBAG_IMPORT_TIMEOUT = conf.getint('core', 'DAGBAG_IMPORT_TIMEOUT')
    UNIT_TEST_MODE = conf.getboolean('core', 'UNIT_TEST_MODE')
    SCHEDULER_ZOMBIE_TASK_THRESHOLD = conf.getint('scheduler', 'scheduler_zombie_task_threshold')

    def __init__(
            self,
            dag_folder=None,
            include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'),
            safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'),
            store_serialized_dags=False,
    ):

        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.dag_folder = dag_folder
        self.dags = {}
        # the file's last modified timestamp when we last read it
        self.file_last_changed = {}
        self.import_errors = {}
        self.has_logged = False
        self.store_serialized_dags = store_serialized_dags

        self.collect_dags(
            dag_folder=dag_folder,
            include_examples=include_examples,
            safe_mode=safe_mode)

    def size(self):
        """
        :return: the amount of dags contained in this dagbag
        """
        return len(self.dags)

    @property
    def dag_ids(self):
        return self.dags.keys()

    def get_dag(self, dag_id, from_file_only=False):
        """
        Gets the DAG out of the dictionary, and refreshes it if expired

        :param dag_id: DAG Id
        :type dag_id: str
        :param from_file_only: returns a DAG loaded from file.
        :type from_file_only: bool
        """
        # Avoid circular import
        from airflow.models.dag import DagModel

        # Only read DAGs from DB if this dagbag is store_serialized_dags.
        # from_file_only is an exception, currently it is for renderring templates
        # in UI only. Because functions are gone in serialized DAGs, DAGs must be
        # imported from files.
        # FIXME: this exception should be removed in future, then webserver can be
        # decoupled from DAG files.
        if self.store_serialized_dags and not from_file_only:
            # Import here so that serialized dag is only imported when serialization is enabled
            from airflow.models.serialized_dag import SerializedDagModel
            if dag_id not in self.dags:
                # Load from DB if not (yet) in the bag
                row = SerializedDagModel.get(dag_id)
                if not row:
                    return None

                dag = row.dag
                for subdag in dag.subdags:
                    self.dags[subdag.dag_id] = subdag
                self.dags[dag.dag_id] = dag

            return self.dags.get(dag_id)

        # If asking for a known subdag, we want to refresh the parent
        dag = None
        root_dag_id = dag_id
        if dag_id in self.dags:
            dag = self.dags[dag_id]
            if dag.is_subdag:
                root_dag_id = dag.parent_dag.dag_id

        # Needs to load from file for a store_serialized_dags dagbag.
        enforce_from_file = False
        if self.store_serialized_dags and dag is not None:
            from airflow.serialization.serialized_objects import SerializedDAG
            enforce_from_file = isinstance(dag, SerializedDAG)

        # If the dag corresponding to root_dag_id is absent or expired
        orm_dag = DagModel.get_current(root_dag_id)
        if (orm_dag and (
                root_dag_id not in self.dags or
                (
                    orm_dag.last_expired and
                    dag.last_loaded < orm_dag.last_expired
                )
        )) or enforce_from_file:
            # Reprocess source file
            found_dags = self.process_file(
                filepath=correct_maybe_zipped(orm_dag.fileloc), only_if_updated=False)

            # If the source file no longer exports `dag_id`, delete it from self.dags
            if found_dags and dag_id in [found_dag.dag_id for found_dag in found_dags]:
                return self.dags[dag_id]
            elif dag_id in self.dags:
                del self.dags[dag_id]
        return self.dags.get(dag_id)

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """
        Given a path to a python module or zip file, this method imports
        the module and look for dag objects within it.
        """
        from airflow.models.dag import DAG  # Avoid circular import

        found_dags = []

        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?
        if filepath is None or not os.path.isfile(filepath):
            return found_dags

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(filepath))
            if only_if_updated \
                    and filepath in self.file_last_changed \
                    and file_last_changed_on_disk == self.file_last_changed[filepath]:
                return found_dags

        except Exception as e:
            self.log.exception(e)
            return found_dags

        mods = []
        is_zipfile = zipfile.is_zipfile(filepath)
        if not is_zipfile:
            if safe_mode:
                with open(filepath, 'rb') as file:
                    content = file.read()
                    if not all([s in content for s in (b'DAG', b'airflow')]):
                        self.file_last_changed[filepath] = file_last_changed_on_disk
                        # Don't want to spam user with skip messages
                        if not self.has_logged:
                            self.has_logged = True
                            self.log.info(
                                "File %s assumed to contain no DAGs. Skipping.",
                                filepath)
                        return found_dags

            self.log.debug("Importing %s", filepath)
            org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
            mod_name = ('unusual_prefix_' +
                        hashlib.sha1(filepath.encode('utf-8')).hexdigest() +
                        '_' + org_mod_name)

            if mod_name in sys.modules:
                del sys.modules[mod_name]

            with timeout(self.DAGBAG_IMPORT_TIMEOUT):
                try:
                    loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
                    spec = importlib.util.spec_from_loader(mod_name, loader)
                    m = importlib.util.module_from_spec(spec)
                    sys.modules[spec.name] = m
                    loader.exec_module(m)
                    mods.append(m)
                except Exception as e:
                    self.log.exception("Failed to import: %s", filepath)
                    self.import_errors[filepath] = str(e)
                    self.file_last_changed[filepath] = file_last_changed_on_disk

        else:
            zip_file = zipfile.ZipFile(filepath)
            for mod in zip_file.infolist():
                head, _ = os.path.split(mod.filename)
                mod_name, ext = os.path.splitext(mod.filename)
                if not head and (ext == '.py' or ext == '.pyc'):
                    if mod_name == '__init__':
                        self.log.warning("Found __init__.%s at root of %s", ext, filepath)
                    if safe_mode:
                        with zip_file.open(mod.filename) as zf:
                            self.log.debug("Reading %s from %s", mod.filename, filepath)
                            content = zf.read()
                            if not all([s in content for s in (b'DAG', b'airflow')]):
                                self.file_last_changed[filepath] = (
                                    file_last_changed_on_disk)
                                # todo: create ignore list
                                # Don't want to spam user with skip messages
                                if not self.has_logged:
                                    self.has_logged = True
                                    self.log.info(
                                        "File %s assumed to contain no DAGs. Skipping.",
                                        filepath)

                    if mod_name in sys.modules:
                        del sys.modules[mod_name]

                    try:
                        sys.path.insert(0, filepath)
                        m = importlib.import_module(mod_name)
                        mods.append(m)
                    except Exception as e:
                        self.log.exception("Failed to import: %s", filepath)
                        self.import_errors[filepath] = str(e)
                        self.file_last_changed[filepath] = file_last_changed_on_disk

        for m in mods:
            for dag in list(m.__dict__.values()):
                if isinstance(dag, DAG):
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                        if dag.fileloc != filepath and not is_zipfile:
                            dag.fileloc = filepath
                    try:
                        dag.is_subdag = False
                        self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                        if isinstance(dag._schedule_interval, str):
                            croniter(dag._schedule_interval)
                        found_dags.append(dag)
                        found_dags += dag.subdags
                    except (CroniterBadCronError,
                            CroniterBadDateError,
                            CroniterNotAlphaError) as cron_e:
                        self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                        self.import_errors[dag.full_filepath] = \
                            "Invalid Cron expression: " + str(cron_e)
                        self.file_last_changed[dag.full_filepath] = \
                            file_last_changed_on_disk
                    except AirflowDagCycleException as cycle_exception:
                        self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                        self.import_errors[dag.full_filepath] = str(cycle_exception)
                        self.file_last_changed[dag.full_filepath] = \
                            file_last_changed_on_disk

        self.file_last_changed[filepath] = file_last_changed_on_disk
        return found_dags

    @provide_session
    def kill_zombies(self, zombies, session=None):
        """
        Fail given zombie tasks, which are tasks that haven't
        had a heartbeat for too long, in the current DagBag.

        :param zombies: zombie task instances to kill.
        :type zombies: List[airflow.models.taskinstance.SimpleTaskInstance]
        :param session: DB session.
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        for zombie in zombies:
            if zombie.dag_id in self.dags:
                dag = self.dags[zombie.dag_id]
                if zombie.task_id in dag.task_ids:
                    task = dag.get_task(zombie.task_id)
                    ti = TaskInstance(task, zombie.execution_date)
                    # Get properties needed for failure handling from SimpleTaskInstance.
                    ti.start_date = zombie.start_date
                    ti.end_date = zombie.end_date
                    ti.try_number = zombie.try_number
                    ti.state = zombie.state
                    ti.test_mode = self.UNIT_TEST_MODE
                    ti.handle_failure("{} detected as zombie".format(ti),
                                      ti.test_mode, ti.get_template_context())
                    self.log.info('Marked zombie job %s as %s', ti, ti.state)
                    Stats.incr('zombies_killed')
        session.commit()

    def bag_dag(self, dag, parent_dag, root_dag):
        """
        Adds the DAG into the bag, recurses into sub dags.
        Throws AirflowDagCycleException if a cycle is detected in this dag or its subdags
        """

        dag.test_cycle()  # throws if a task cycle is found

        dag.resolve_template_files()
        dag.last_loaded = timezone.utcnow()

        for task in dag.tasks:
            settings.policy(task)

        subdags = dag.subdags

        try:
            for subdag in subdags:
                subdag.full_filepath = dag.full_filepath
                subdag.parent_dag = dag
                subdag.is_subdag = True
                self.bag_dag(subdag, parent_dag=dag, root_dag=root_dag)

            self.dags[dag.dag_id] = dag
            self.log.debug('Loaded DAG %s', dag)
        except AirflowDagCycleException as cycle_exception:
            # There was an error in bagging the dag. Remove it from the list of dags
            self.log.exception('Exception bagging dag: %s', dag.dag_id)
            # Only necessary at the root level since DAG.subdags automatically
            # performs DFS to search through all subdags
            if dag == root_dag:
                for subdag in subdags:
                    if subdag.dag_id in self.dags:
                        del self.dags[subdag.dag_id]
            raise cycle_exception

    def collect_dags(
            self,
            dag_folder=None,
            only_if_updated=True,
            include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'),
            safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE')):
        """
        Given a file path or a folder, this method looks for python modules,
        imports them and adds them to the dagbag collection.

        Note that if a ``.airflowignore`` file is found while processing
        the directory, it will behave much like a ``.gitignore``,
        ignoring files that match any of the regex patterns specified
        in the file.

        **Note**: The patterns in .airflowignore are treated as
        un-anchored regexes, not shell-like glob patterns.
        """
        if self.store_serialized_dags:
            return

        self.log.info("Filling up the DagBag from %s", dag_folder)
        start_dttm = timezone.utcnow()
        dag_folder = dag_folder or self.dag_folder
        # Used to store stats around DagBag processing
        stats = []

        from airflow.utils.file import correct_maybe_zipped, list_py_file_paths
        dag_folder = correct_maybe_zipped(dag_folder)
        for filepath in list_py_file_paths(dag_folder, safe_mode=safe_mode,
                                           include_examples=include_examples):
            try:
                ts = timezone.utcnow()
                found_dags = self.process_file(
                    filepath, only_if_updated=only_if_updated,
                    safe_mode=safe_mode)
                dag_ids = [dag.dag_id for dag in found_dags]
                dag_id_names = str(dag_ids)

                td = timezone.utcnow() - ts
                stats.append(FileLoadStat(
                    filepath.replace(settings.DAGS_FOLDER, ''),
                    td,
                    len(found_dags),
                    sum([len(dag.tasks) for dag in found_dags]),
                    dag_id_names,
                ))
            except Exception as e:
                self.log.exception(e)
        Stats.gauge(
            'collect_dags', (timezone.utcnow() - start_dttm).total_seconds(), 1)
        Stats.gauge('dagbag_size', len(self.dags), 1)
        Stats.gauge('dagbag_import_errors', len(self.import_errors), 1)
        self.dagbag_stats = sorted(
            stats, key=lambda x: x.duration, reverse=True)
        for file_stat in self.dagbag_stats:
            # file_stat.file similar format: /subdir/dag_name.py
            # TODO: Remove for Airflow 2.0
            filename = file_stat.file.split('/')[-1].replace('.py', '')
            Stats.timing('dag.loading-duration.{}'.
                         format(filename),
                         file_stat.duration)

    def collect_dags_from_db(self):
        """Collects DAGs from database."""
        from airflow.models.serialized_dag import SerializedDagModel
        start_dttm = timezone.utcnow()
        self.log.info("Filling up the DagBag from database")

        # The dagbag contains all rows in serialized_dag table. Deleted DAGs are deleted
        # from the table by the scheduler job.
        self.dags = SerializedDagModel.read_all_dags()

        # Adds subdags.
        # DAG post-processing steps such as self.bag_dag and croniter are not needed as
        # they are done by scheduler before serialization.
        subdags = {}
        for dag in self.dags.values():
            for subdag in dag.subdags:
                subdags[subdag.dag_id] = subdag
        self.dags.update(subdags)

        Stats.timing('collect_db_dags', timezone.utcnow() - start_dttm)

    def dagbag_report(self):
        """Prints a report around DagBag loading stats"""
        report = textwrap.dedent("""\n
        -------------------------------------------------------------------
        DagBag loading stats for {dag_folder}
        -------------------------------------------------------------------
        Number of DAGs: {dag_num}
        Total task number: {task_num}
        DagBag parsing time: {duration}
        {table}
        """)
        stats = self.dagbag_stats
        return report.format(
            dag_folder=self.dag_folder,
            duration=sum([o.duration for o in stats], timedelta()).total_seconds(),
            dag_num=sum([o.dag_num for o in stats]),
            task_num=sum([o.task_num for o in stats]),
            table=pprinttable(stats),
        )
