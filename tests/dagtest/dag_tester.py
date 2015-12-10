"""
General entry point for testing end to end dags
"""
import logging
import shutil
import tempfile
import time
import unittest

import os
import re
from airflow import configuration
from airflow import executors
from airflow.configuration import TEST_CONFIG_FILE, get_sql_alchemy_conn
from airflow.jobs import BackfillJob, SchedulerJob
from airflow.models import DagBag, Variable, DagRun, TaskInstance
from airflow.utils.logging import LoggingMixin
from airflow.utils.db import provide_session


def is_config_db_concurrent():
    """
    :return: true if the sqlalchemy connection is set up to use a DB that
    supports concurrent access (i.e. not sqlite)
    """
    return "sqlite://" not in get_sql_alchemy_conn()


class AbstractEndToEndTest(LoggingMixin):
    """
    Convenience super class with common abstract methods between
    EndToEndBackfillJobTest and EndToEndSchedulerJobTest
    """

    def get_dag_file_names(self):
        """
        :return: a non empty list of python file names containing dag(s) to
        be tested in the context of this test.
        """

        raise NotImplementedError()

    def get_context(self):
        """
        :return: a dictionary of variables to be stored such that the
        tested DAG can access them through a Variable.get("key") statement
        """
        return {}

    def post_check(self, working_dir):
        """
        :param working_dir: the tmp file where the tested DAG has been
        executed

        Child classes should implement here any post-check and raise
        exceptions via assertions to trigger a test failure.
        """

        raise NotImplementedError()


class EndToEndBackfillJobTest(AbstractEndToEndTest):
    """
    Abstract class to implement in order to execute an end-to-end DAG test based
    on a BackfillJob.
    """

    def get_backfill_params(self):
        """
        :return: dictionary **kwargs argument for building the BackfillJob
        execution of this test.
        """
        raise NotImplementedError()

    @unittest.skipIf(not is_config_db_concurrent(),
                     "DB Backend must support concurrent access")
    def test_backfilljob(self):

        with BackFillJobRunner(self.get_backfill_params(),
                               dag_file_names=self.get_dag_file_names(),
                               context=self.get_context()) as runner:

            runner.run()
            self.post_check(runner.working_dir)


class EndToEndSchedulerJobTest(AbstractEndToEndTest):
    """
    Abstract class to implement in order to execute an end-to-end DAG test based
    on a SchedulerJob.
    """

    def get_schedulerjob_params(self):
        """
        :return: dictionary **kwargs argument for building the BackfillJob
        execution of this test.
        """
        raise NotImplementedError()

    @unittest.skipIf(not is_config_db_concurrent(),
                     "DB Backend must support concurrent access")
    def test_schedulerjob(self):

        with SchedulerJobRunner(self.get_schedulerjob_params(),
                                dag_file_names=self.get_dag_file_names(),
                                context=self.get_context()) as runner:

            runner.run()
            self.post_check(runner.working_dir)


class Runner(object):
    """
    Abstract Runner that prepares a working temp dir and all necessary context
    variables in order to execute a job in its own isolated folder.
    """

    def __init__(self,
                 dag_file_names,
                 context=None):

        self.dag_file_names = dag_file_names

        # makes sure the default context is a different instance for each Runner
        self.context = context if context else {}

        # this is initialized in the constructor of the child class
        self.tested_job = None

        # preparing a folder where to execute the tests, with all the DAGs
        all_dags_folder = os.path.join(os.path.dirname(__file__), "dags")
        self.working_dir = tempfile.mkdtemp()
        self.it_dag_folder = os.path.join(self.working_dir, "dags")
        os.mkdir(self.it_dag_folder)
        for file_name in self.dag_file_names:
            src = os.path.join(all_dags_folder, file_name)
            shutil.copy2(src, self.it_dag_folder)

        # saving the context to Variable so the child test can access it
        # while saving existing Variables if the test would be overwriting any
        self.context['unit_test_tmp_dir'] = self.working_dir
        self.saved_variables = {}
        for key, val in self.context.items():
            try:
                old_value = Variable.get(key)
                self.saved_variables[key] = old_value
            except ValueError:
                pass
            Variable.set(key, val, serialize_json=True)

        self.config_file = self._create_it_config_file(self.it_dag_folder)

        # aligns current config with test config (this of course would fail
        # if several dag tests are executed in parallel threads)
        configuration.AIRFLOW_CONFIG = self.config_file
        configuration.load_config()

        self.dagbag = DagBag(self.it_dag_folder, include_examples=False)

        # environment variables for the child processes launched by the test
        self.test_env = os.environ.copy()
        self.test_env.update({"AIRFLOW_CONFIG": self.config_file})

        self._reset_dags()

    def run(self):
        """
        Starts the execution of the tested job.
        """
        self.tested_job.run()

    def cleanup(self):
        """
        Deletes all traces of execution of the tested job.
        This is called automatically if the Runner is used inside a with
        statement
        """
        logging.info("cleaning up {}".format(self.tested_job))
        self._reset_dags()
        os.system("rm -rf {}".format(self.working_dir))

        # Restore Variables that were overwritten
        for key, val in self.saved_variables.items():
            Variable.set(key, val)

    def dag_ids(self):
        """
        :return: the set of all dag_ids tested by the test
        """
        return self.dagbag.dags.keys()

    ##########################
    # private methods

    @provide_session
    def _reset_dags(self, session=None):
        for dag_id in self.dag_ids():
            session.query(TaskInstance).filter_by(dag_id=dag_id).delete()
            session.query(DagRun).filter_by(dag_id=dag_id).delete()

    def _create_it_config_file(self, dag_folder):
        """
        Creates a custom config file for integration tests in the specified
        location, overriding the dag_folder and heartbeat_sec values.
        """

        it_file_location = os.path.join(self.working_dir, "airflow_IT.cfg")

        with open(TEST_CONFIG_FILE) as test_config_file:
            config = test_config_file.read()

            config = re.sub("dags_folder =.*",
                            "dags_folder = {}".format(dag_folder), config)
            config = re.sub("job_heartbeat_sec =.*",
                            "job_heartbeat_sec = 1", config)
            config = re.sub("load_examples =.*",
                            "load_examples = False", config)

            # this is the config file that will be used by the child process
            with open(it_file_location, "w") as cfg_file:
                cfg_file.write(config)

        return it_file_location

    ###########################
    # loan pattern to make any runner easily usable inside a with statement

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class BackFillJobRunner(Runner):
    """
    Executes a BackfillJob on the specified dagfiles, in its own tmp folder,
    with all necessary Variables specified in context persisted so that the
    child context has access to it.
    """

    def __init__(self, backfilljob_params, **kwargs):
        super(BackFillJobRunner, self).__init__(**kwargs)

        if self.dagbag.size() > 1:
            self.cleanup()
            assert False, "more than one dag found in BackfillJob test"

        self.dag = list(self.dagbag.dags.values())[0]
        self.tested_job = BackfillJob(dag=self.dag, **backfilljob_params)
        self.tested_job.executor = executors.SequentialExecutor(
            env=self.test_env)

        self.tested_job.dag.clear()


class SchedulerJobRunner(Runner):
    """
    Executes a SchedulerJob on the specified dagfiles, in its own tmp folder,
    with all necessary Variables specified in context persisted so that the
    child context has access to it.
    """

    def __init__(self, job_params, **kwargs):
        super(SchedulerJobRunner, self).__init__(**kwargs)

        self.tested_job = SchedulerJob(subdir=self.it_dag_folder, **job_params)
        self.tested_job.executor = executors.LocalExecutor(env=self.test_env)


#############
# some useful post-check validation utils

def get_existing_files_in_folder(folder):
    return [f for f in os.listdir(folder)
            if os.path.isfile(os.path.join(folder, f))]


def validate_file_content(folder, filename, expected_content):
    """
    Raise an exception if the specified file does not have the expected
    content, or returns silently otherwise
    """
    path = "{0}/{1}".format(folder, filename)
    if not os.path.isfile(path):
        folder_content = "\n".join(get_existing_files_in_folder(folder))
        assert False, "File {path} does not exist. Here are the existing " \
                      "files :\n{folder_content}".format(**locals())
    with open(path) as f:
        content = f.read()
        assert expected_content == content, \
            "Unexpected content of {path}\n" \
            "  Expected content : {expected_content}\n" \
            "  Actual content : {content}".format(**locals())


def validate_order(folder, early, late):
    """
    Raise an exception if the last modification of the early file happened
    after the last modification of the late file
    """
    path_early = "{0}/{1}".format(folder, early)
    path_late = "{0}/{1}".format(folder, late)

    time_early = time.ctime(os.path.getmtime(path_early))
    time_late = time.ctime(os.path.getmtime(path_late))
    assert time_early < time_late, \
        "The last modification time of {path_early} should be before the " \
        "last modification time of {path_late} but it was not the case." \
        "".format(**locals())
