"""
General entry point for testing end to end dags
"""
import logging
import os
import re
import tempfile
import time

from airflow import configuration, AirflowException
from airflow import executors
from airflow.configuration import AIRFLOW_HOME, TEST_CONFIG_FILE
from airflow.models import DagBag, Variable
from ..core import reset


class DagBackfillTest(object):
    """
    Framework to setup, run and check end to end executions of a DAG controlled.

    Usage: just create a sub-class and implement build_job(), get_dag_id(),
    post_check() and optionally get_test_context()
    """

    ###############
    # methods to be implemented by child test

    def build_job(self, dag):
        raise NotImplementedError()

    def get_dag_id(self):
        raise NotImplementedError()

    def post_check(self, working_dir):
        raise NotImplementedError()

    def get_test_context(self):
        """
        :return: a dictionary of variables to be stored such that the
        tested DAG can access them through a Variable.get("key") statement
        """
        return {}

    ################################

    def test_run(self):

        # init
        ctx = self._init_full_context()
        tested_job = self._build_tested_job()

        reset(tested_job.dag.dag_id)
        tested_job.dag.clear()
        
        # run
        try:
            tested_job.run()
        except SystemExit:
            logging.warn("Tested job has failed (this might be ok if the "
                         "test is validating a failure condition)")

        # cleanup
        temp_dir = ctx["unit_test_tmp_dir"]
        self.post_check(temp_dir)
        os.system("rm -rf {}".format(temp_dir))
        reset(tested_job.dag.dag_id)

    ################################

    def _init_full_context(self):
        """
        Merge the default context (including the temp test dir) with the test
        specific context and stores everything in persistent Variables in DB
        :return: the merged context
        """

        full_context = self.get_test_context().copy()

        tmp_dir = tempfile.mkdtemp()
        full_context["unit_test_tmp_dir"] = tmp_dir

        for key, val in full_context.items():
            Variable.set(key, val, serialize_json=True)

        return full_context

    def _dag_folder(self):
        """
        :return: the location where to find the tested dags
        """
        return "{}/dags".format(os.path.dirname(__file__))

    def _build_tested_job(self):
        """
        Builds a job for this test (actually just some boiler plate here, the
        actual creation is done by the child class in build_job())
        """

        dagbag = DagBag(self._dag_folder(), include_examples=False)

        if self.get_dag_id() not in dagbag.dags:
            msg = "DAG id {id} not found in folder {folder}" \
                  "".format(id=self.get_dag_id(), folder=self._dag_folder())
            raise AirflowException(msg)

        dag = dagbag.dags[self.get_dag_id()]
        job = self.build_job(dag)

        if job.executor != executors.DEFAULT_EXECUTOR:
            raise AirflowException("DAG test may not set the executor")

        config_location = self._create_ut_config_file(self._dag_folder())
        test_env = os.environ.copy()
        test_env.update({"AIRFLOW_CONFIG": config_location})
        job.executor = executors.SequentialExecutor(env=test_env)

        return job

    def _create_ut_config_file(self, dags_folder):
        """
        Creates a custom config file called dag_test_airflow.cfg so that
        the child OS process launched during the test to execute the DAG
        has the correct DAG folder.
        """

        with open(TEST_CONFIG_FILE) as test_config_file:
            config = test_config_file.read()

            config = re.sub("dags_folder =.*",
                            "dags_folder = {}".format(dags_folder), config)
            config = re.sub("job_heartbeat_sec =.*",
                            "job_heartbeat_sec = 1", config)

            # this is the config file that will be used by the child process
            config_location = "{}/dag_test_airflow.cfg".format(AIRFLOW_HOME)
            with open(config_location, "w") as cfg_file:
                cfg_file.write(config)

        # aligns current config with test config
        configuration.conf.set("core", "DAGS_FOLDER", dags_folder)

        return config_location


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
