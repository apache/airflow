import unittest
from datetime import datetime

from airflow.bin import cli
from airflow.jobs import BaseJob
from airflow.models import TaskInstance
from airflow.settings import Session
from airflow.utils import State
from ..dag_tester import BackFillJobRunner, is_config_db_concurrent


class ClearTests(unittest.TestCase):

    skip_message = "DB Backend must support concurrent access"

    @classmethod
    def setUpClass(cls):

        if is_config_db_concurrent():
            backfill_params = {"start_date": datetime(2015, 1, 1),
                               "end_date": datetime(2015, 1, 5)}

            dag_file_names = ["bash_operator_abc.py"]

            # Run a fake job to make get an initial DB state we can work with
            cls._runner = BackFillJobRunner(backfilljob_params=backfill_params,
                                            dag_file_names=dag_file_names)

            cls._dagbag = cls._runner.dagbag
            cls._dag_id = cls._runner.dag.dag_id
            cls._dag_folder = "DAGS_FOLDER/{}".format(dag_file_names[0])

            cls._runner.run()

            session = Session()

            # Make sure all went well with job
            tis = session.query(TaskInstance)\
                .filter(TaskInstance.dag_id == cls._dag_id)\
                .all()

            assert len(tis) == 5*3
            for task_instance in tis:
                assert task_instance.state == State.SUCCESS

            job_ids = [ti.job_id for ti in tis]
            jobs = session.query(BaseJob) \
                .filter(BaseJob.id.in_(job_ids)) \
                .all()

            for job in jobs:
                assert job.state == State.SUCCESS

            session.close()

            # Save DB state
            cls._original_task_instances = tis

    @classmethod
    def tearDownClass(cls):
        if is_config_db_concurrent():
            cls._runner.cleanup()

    def setUp(self):

        session = Session()
        # Restore DB to initial state
        for original_task_instance in self._original_task_instances:
            session.merge(original_task_instance)
        session.commit()
        session.close()

        self.parser = cli.get_parser()

    def get_tis(self):
        session = Session()
        tis = session.query(TaskInstance) \
            .filter(TaskInstance.dag_id == self._dag_id) \
            .all()
        session.close()
        return tis

    def assert_all_tis_are_successful(self, tis):
        for task_instance in tis:
            assert task_instance.state == State.SUCCESS

    def assert_find_ti_with(self, tis, date, task_id):
        found = False
        for ti in tis:
            if ti.execution_date == date and ti.task_id == task_id:
                found = True
                break
        assert found, "{}:{} was not found".format(task_id, date)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_all(self):

        cli.clear(self.parser.parse_args([
            'clear',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        tis = self.get_tis()
        assert len(tis) == 0

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start(self):

        start_date = datetime(2015, 1, 3).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-s', start_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_dates = [datetime(2015, 1, 1), datetime(2015, 1, 2)]
        remaining_task_ids = ['echo_a', 'echo_b', 'echo_c']

        tis = self.get_tis()
        assert len(tis) == len(remaining_dates) * len(remaining_task_ids)
        self.assert_all_tis_are_successful(tis)

        for date in remaining_dates:
            for task_id in remaining_task_ids:
                self.assert_find_ti_with(tis, date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_end(self):

        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_dates = [datetime(2015, 1, 5)]
        remaining_task_ids = ['echo_a', 'echo_b', 'echo_c']

        tis = self.get_tis()
        assert len(tis) == len(remaining_dates) * len(remaining_task_ids)
        self.assert_all_tis_are_successful(tis)

        for date in remaining_dates:
            for task_id in remaining_task_ids:
                self.assert_find_ti_with(tis, date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_end(self):

        start_date = datetime(2015, 1, 3).isoformat()
        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-s', start_date,
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_dates = [datetime(2015, 1, 1),
                           datetime(2015, 1, 2),
                           datetime(2015, 1, 5)]
        remaining_task_ids = ['echo_a', 'echo_b', 'echo_c']

        tis = self.get_tis()
        assert len(tis) == len(remaining_dates) * len(remaining_task_ids)
        self.assert_all_tis_are_successful(tis)

        for date in remaining_dates:
            for task_id in remaining_task_ids:
                self.assert_find_ti_with(tis, date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_end_task(self):

        start_date = datetime(2015, 1, 3).isoformat()
        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-s', start_date,
            '-e', end_date,
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        all_dates = [datetime(2015, 1, 1),
                     datetime(2015, 1, 2),
                     datetime(2015, 1, 3),
                     datetime(2015, 1, 4),
                     datetime(2015, 1, 5)]

        remaining_dates = [datetime(2015, 1, 1),
                           datetime(2015, 1, 2),
                           datetime(2015, 1, 5)]

        tis = self.get_tis()
        assert len(tis) == len(remaining_dates) + 2*len(all_dates)
        self.assert_all_tis_are_successful(tis)

        for date in all_dates:
            self.assert_find_ti_with(tis, date, 'echo_a')
            self.assert_find_ti_with(tis, date, 'echo_c')

        for date in remaining_dates:
            self.assert_find_ti_with(tis, date, 'echo_b')

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_start_task_upstream(self):

        start_date = datetime(2015, 1, 3).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-s', start_date,
            '--upstream',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_task_and_dates = [('echo_a', datetime(2015, 1, 1)),
                                    ('echo_b', datetime(2015, 1, 1)),
                                    ('echo_c', datetime(2015, 1, 1)),
                                    ('echo_a', datetime(2015, 1, 2)),
                                    ('echo_b', datetime(2015, 1, 2)),
                                    ('echo_c', datetime(2015, 1, 2)),
                                    ('echo_c', datetime(2015, 1, 3)),
                                    ('echo_c', datetime(2015, 1, 4)),
                                    ('echo_c', datetime(2015, 1, 5))]

        tis = self.get_tis()
        assert len(tis) == len(remaining_task_and_dates)
        self.assert_all_tis_are_successful(tis)

        for (task_id, date) in remaining_task_and_dates:
            self.assert_find_ti_with(tis, date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_end_task_downstream(self):

        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-e', end_date,
            '--downstream',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_task_and_dates = [('echo_a', datetime(2015, 1, 1)),
                                    ('echo_a', datetime(2015, 1, 2)),
                                    ('echo_a', datetime(2015, 1, 3)),
                                    ('echo_a', datetime(2015, 1, 4)),
                                    ('echo_a', datetime(2015, 1, 5)),
                                    ('echo_b', datetime(2015, 1, 5)),
                                    ('echo_c', datetime(2015, 1, 5))]

        tis = self.get_tis()
        assert len(tis) == len(remaining_task_and_dates)
        self.assert_all_tis_are_successful(tis)

        for (task_id, date) in remaining_task_and_dates:
            self.assert_find_ti_with(tis, date, task_id)

    @unittest.skipIf(not is_config_db_concurrent(), skip_message)
    def test_clear_with_end_task_upstream_downstream(self):

        end_date = datetime(2015, 1, 4).isoformat()
        cli.clear(self.parser.parse_args([
            'clear',
            '-t', 'echo_b',
            '-e', end_date,
            '--upstream',
            '--downstream',
            '-sd', self._dag_folder,
            '--no_confirm',
            self._dag_id]))

        remaining_task_and_dates = [('echo_a', datetime(2015, 1, 5)),
                                    ('echo_b', datetime(2015, 1, 5)),
                                    ('echo_c', datetime(2015, 1, 5))]

        tis = self.get_tis()
        assert len(tis) == len(remaining_task_and_dates)
        self.assert_all_tis_are_successful(tis)

        for (task_id, date) in remaining_task_and_dates:
            self.assert_find_ti_with(tis, date, task_id)

