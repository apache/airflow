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

import copy
import logging.config

import dateutil
import mock
import os
import shutil
import tempfile
import unittest
from datetime import datetime
import sys

import requests

from airflow import models, configuration, settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.settings import Session
from airflow.www import app as application
from airflow import configuration as conf


class TestChartModelView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/chart/new/?url=/admin/chart/'

    @classmethod
    def setUpClass(cls):
        super(TestChartModelView, cls).setUpClass()
        session = Session()
        session.query(models.Chart).delete()
        session.query(models.User).delete()
        session.commit()
        user = models.User(username='airflow')
        session.add(user)
        session.commit()
        session.close()

    def setUp(self):
        super(TestChartModelView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.chart = {
            'label': 'chart',
            'owner': 'airflow',
            'conn_id': 'airflow_ci',
        }

    def tearDown(self):
        self.session.query(models.Chart).delete()
        self.session.commit()
        self.session.close()
        super(TestChartModelView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        super(TestChartModelView, cls).tearDownClass()

    def test_create_chart(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.chart,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.Chart).count(), 1)

    def test_get_chart(self):
        response = self.app.get(
            '/admin/chart?sort=3',
            follow_redirects=True,
        )
        print(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertIn('Sort by Owner', response.data.decode('utf-8'))


class TestVariableView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/variable/new/?url=/admin/variable/'

    @classmethod
    def setUpClass(cls):
        super(TestVariableView, cls).setUpClass()
        session = Session()
        session.query(models.Variable).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestVariableView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.variable = {
            'key': 'test_key',
            'val': 'text_val',
            'is_encrypted': True
        }

    def tearDown(self):
        self.session.query(models.Variable).delete()
        self.session.commit()
        self.session.close()
        super(TestVariableView, self).tearDown()

    def test_can_handle_error_on_decrypt(self):
        # create valid variable
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.variable,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        # update the variable with a wrong value, given that is encrypted
        Var = models.Variable
        (self.session.query(Var)
            .filter(Var.key == self.variable['key'])
            .update({
                'val': 'failed_value_not_encrypted'
            }, synchronize_session=False))
        self.session.commit()

        # retrieve Variables page, should not fail and contain the Invalid
        # label for the variable
        response = self.app.get('/admin/variable', follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.Variable).count(), 1)
        self.assertIn('<span class="label label-danger">Invalid</span>',
                      response.data.decode('utf-8'))

    def test_xss_prevention(self):
        xss = "/admin/airflow/variables/asdf<img%20src=''%20onerror='alert(1);'>"

        response = self.app.get(
            xss,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("<img src='' onerror='alert(1);'>",
                         response.data.decode("utf-8"))


class TestKnownEventView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/knownevent/new/?url=/admin/knownevent/'

    @classmethod
    def setUpClass(cls):
        super(TestKnownEventView, cls).setUpClass()
        session = Session()
        session.query(models.KnownEvent).delete()
        session.query(models.User).delete()
        session.commit()
        user = models.User(username='airflow')
        session.add(user)
        session.commit()
        cls.user_id = user.id
        session.close()

    def setUp(self):
        super(TestKnownEventView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.known_event = {
            'label': 'event-label',
            'event_type': '1',
            'start_date': '2017-06-05 12:00:00',
            'end_date': '2017-06-05 13:00:00',
            'reported_by': self.user_id,
            'description': '',
        }

    def tearDown(self):
        self.session.query(models.KnownEvent).delete()
        self.session.commit()
        self.session.close()
        super(TestKnownEventView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        super(TestKnownEventView, cls).tearDownClass()

    def test_create_known_event(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.known_event,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.KnownEvent).count(), 1)

    def test_create_known_event_with_end_data_earlier_than_start_date(self):
        self.known_event['end_date'] = '2017-06-05 11:00:00'
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.known_event,
            follow_redirects=True,
        )
        self.assertIn(
            'Field must be greater than or equal to Start Date.',
            response.data.decode('utf-8'),
        )
        self.assertEqual(self.session.query(models.KnownEvent).count(), 0)


class TestPoolModelView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/pool/new/?url=/admin/pool/'

    @classmethod
    def setUpClass(cls):
        super(TestPoolModelView, cls).setUpClass()
        session = Session()
        session.query(models.Pool).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestPoolModelView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.pool = {
            'pool': 'test-pool',
            'slots': 777,
            'description': 'test-pool-description',
        }

    def tearDown(self):
        self.session.query(models.Pool).delete()
        self.session.commit()
        self.session.close()
        super(TestPoolModelView, self).tearDown()

    def test_create_pool(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_create_pool_with_same_name(self):
        # create test pool
        self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        # create pool with the same name
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertIn('Already exists.', response.data.decode('utf-8'))
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_create_pool_with_empty_name(self):
        self.pool['pool'] = ''
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertIn('This field is required.', response.data.decode('utf-8'))
        self.assertEqual(self.session.query(models.Pool).count(), 0)


class TestLogView(unittest.TestCase):
    DAG_ID = 'dag_for_testing_log_view'
    TASK_ID = 'task_for_testing_log_view'
    DEFAULT_DATE = datetime(2017, 9, 1)
    ENDPOINT = '/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}'.format(
        dag_id=DAG_ID,
        task_id=TASK_ID,
        execution_date=DEFAULT_DATE,
    )

    # need to have a Datetime object for database type filters, but can't use it in execution_time because
    # it appends the time component when you make it a string and defeats the test
    DATE_WITHOUT_TIME_DT = datetime(2017, 9, 2)
    DATE_WITHOUT_TIME = datetime.date(DATE_WITHOUT_TIME_DT)
    WITHOUT_TIME_ENDPOINT = '/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}'.format(
        dag_id=DAG_ID,
        task_id=TASK_ID,
        execution_date=DATE_WITHOUT_TIME,
    )

    @classmethod
    def setUpClass(cls):
        super(TestLogView, cls).setUpClass()
        session = Session()
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == cls.DAG_ID and
            TaskInstance.task_id == cls.TASK_ID and
            TaskInstance.execution_date.in_(cls.DEFAULT_DATE, cls.DATE_WITHOUT_TIME_DT)).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestLogView, self).setUp()

        # Create a custom logging configuration
        configuration.load_test_config()
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        logging_config['handlers']['file.task']['base_log_folder'] = os.path.normpath(
            os.path.join(current_dir, 'test_logs'))
        logging_config['handlers']['file.task']['filename_template'] = \
            '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts | replace(":", ".") }}/{{ try_number }}.log'

        # Write the custom logging configuration to a file
        self.settings_folder = tempfile.mkdtemp()
        settings_file = os.path.join(self.settings_folder, "airflow_local_settings.py")
        new_logging_file = "LOGGING_CONFIG = {}".format(logging_config)
        with open(settings_file, 'w') as handle:
            handle.writelines(new_logging_file)
        sys.path.append(self.settings_folder)
        conf.set('core', 'logging_config_class', 'airflow_local_settings.LOGGING_CONFIG')

        app = application.create_app(testing=True)
        self.app = app.test_client()
        self.session = Session()
        from airflow.www.views import dagbag
        dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
        task = DummyOperator(task_id=self.TASK_ID, dag=dag)
        dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
        ti = TaskInstance(task=task, execution_date=self.DEFAULT_DATE)
        ti.try_number = 1
        self.session.merge(ti)
        ti_wo_time = TaskInstance(task=task, execution_date=self.DATE_WITHOUT_TIME)
        ti_wo_time.try_number = 1
        self.session.merge(ti_wo_time)
        self.session.commit()

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        dagbag = models.DagBag(settings.DAGS_FOLDER)
        self.session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.DAG_ID and
            TaskInstance.task_id == self.TASK_ID and
            TaskInstance.execution_date.in_(self.DEFAULT_DATE, self.DATE_WITHOUT_TIME_DT)).delete()
        self.session.commit()
        self.session.close()

        sys.path.remove(self.settings_folder)
        shutil.rmtree(self.settings_folder)
        conf.set('core', 'logging_config_class', '')

        super(TestLogView, self).tearDown()

    def test_get_file_task_log(self):
        response = self.app.get(
            TestLogView.ENDPOINT,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn('<pre id="attempt-1">*** Reading local log.\nLog for testing.\n</pre>',
                      response.data.decode('utf-8'))

    def test_get_bad_file_task_log(self):
        """
        The purpose of this test is for AIRFLOW-1735. An execution_date in the form YYYY-MM-DD needs to add
        the time component (in isoformat) in order to retrieve the correct log for that task instance. After
        the changes, this test should show passing in a execution_date in YYYY-MM-DD will resolve to
        YYYY-MM-DDTHH:MM:SS to match the log name and display the log contents.
        """
        response = self.app.get(
            TestLogView.WITHOUT_TIME_ENDPOINT,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(self.DATE_WITHOUT_TIME_DT.isoformat(), response.data.decode('utf-8'))
        self.assertIn(self.DATE_WITHOUT_TIME_DT.isoformat().replace('T', ' '), response.data.decode('utf-8'))
        self.assertIn(self.DATE_WITHOUT_TIME_DT.isoformat().replace(':', '%3A'), response.data.decode('utf-8'))


def mocked_requests_get(*args, **kwargs):
    """
    Function and Class are just to mock a request.get for the LogViewExecutionDate.test_log_execution_date.
    """
    class MockResponse:
        def __init__(self, json_data, status_code, params):
            self.json_data = json_data
            self.status_code = status_code
            self.args = params

    return MockResponse(json_data=None, status_code=200, params=kwargs['params'])


class LogViewExecutionDate:
    """
    Class is to mock parts of the LogView function that handles pulling the execution date from the request
    and putting it in the proper format.
    """
    def __init__(self, execution_date=None):
        self.execution_date = execution_date

    def get_log_execution_date(self, request):
        """
        AIRFLOW-1735.
        :param request: Mocked request
        :return: Execution date in YYYY-MM-DDTHH:mm:ss format.
        """
        dttm = dateutil.parser.parse(request.args.get('execution_date'))
        execution_date = dttm.isoformat()
        self.execution_date = execution_date
        return self.execution_date

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_log_execution_date(self, mock_get):
        base_log_endpoint = '{0}/admin/airflow/log'.format(configuration.get('webserver', 'base_url'))
        data = {
            'task_id': 'run_this_first',
            'dag_id': 'example_branch_operator',
            'execution_date': '2017-10-20',
        }
        req = requests.get(url=base_log_endpoint, params=data)
        return self.get_log_execution_date(req)


class MockLogView(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    def runTest(self):
        DATE = '2017-10-20'
        view = LogViewExecutionDate(execution_date=DATE)
        execution_date = view.test_log_execution_date()
        self.assertEqual(execution_date, '2017-10-20T00:00:00')
        self.assertNotEqual(execution_date, DATE)

if __name__ == '__main__':
    unittest.main()
