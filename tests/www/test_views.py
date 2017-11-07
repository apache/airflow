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
import os
import shutil
import tempfile
import unittest
from datetime import datetime
import sys

from airflow import models, configuration, settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.settings import Session
from airflow.www import app as application
from airflow.utils.state import State
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

    @classmethod
    def setUpClass(cls):
        super(TestLogView, cls).setUpClass()
        session = Session()
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == cls.DAG_ID and
            TaskInstance.task_id == cls.TASK_ID and
            TaskInstance.execution_date == cls.DEFAULT_DATE).delete()
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
        self.session.commit()

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        dagbag = models.DagBag(settings.DAGS_FOLDER)
        self.session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.DAG_ID and
            TaskInstance.task_id == self.TASK_ID and
            TaskInstance.execution_date == self.DEFAULT_DATE).delete()
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


class TestTaskInstanceStateView(unittest.TestCase):
    """Tests if the state of a task instance can be successfully set."""
    TI_ENDPOINT = '/admin/taskinstance/action/'
    DAG_ID = 'dag_ti_without_frac_time'
    TASK_ID = 'task_ti_without_frac_time'
    DATE = datetime(2017, 10, 23)
    DAG_ID_FRAC = 'dag_ti_frac_time'
    TASK_ID_FRAC = 'task_ti_frac_time'
    DATE_FRAC = datetime(2017, 10, 23, 12, 34, 22, 100)

    @staticmethod
    def findTaskInstances(session, dagId, taskId, dtime):
        return session.query(TaskInstance).filter(
            TaskInstance.dag_id == dagId and
            TaskInstance.task_id == taskId and
            TaskInstance.execution_date == dtime)

    @classmethod
    def setUpClass(cls):
        super(TestTaskInstanceStateView, cls).setUpClass()
        session = Session()
        cls.findTaskInstances(session,
                              cls.DAG_ID, cls.TASK_ID, cls.DATE).delete()
        cls.findTaskInstances(session,
                              cls.DAG_ID_FRAC,
                              cls.TASK_ID_FRAC,
                              cls.DATE_FRAC).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestTaskInstanceStateView, self).setUp()
        ## Setting up a test app, Session, DAGs and task instances.
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        ## Creating DAGs and task instances.
        ## DAG and TI without fractional time.
        from airflow.www.views import dagbag
        dag = DAG(self.DAG_ID, start_date=self.DATE)
        task = DummyOperator(task_id=self.TASK_ID, dag=dag)
        dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
        ti = TaskInstance(task=task, execution_date=self.DATE)
        self.session.merge(ti)
        ## DAG and TI with fractional time.
        dag_frac = DAG(self.DAG_ID_FRAC, start_date=self.DATE_FRAC)
        task_frac = DummyOperator(task_id=self.TASK_ID_FRAC, dag=dag_frac)
        dagbag.bag_dag(dag_frac, parent_dag=dag_frac, root_dag=dag_frac)
        ti_frac = TaskInstance(task=task_frac,
                               execution_date=self.DATE_FRAC)
        self.session.merge(ti_frac)
        self.session.commit()

    def tearDown(self):
        self.findTaskInstances(self.session,
                               self.DAG_ID, self.TASK_ID, self.DATE).delete()
        self.findTaskInstances(self.session,
                               self.DAG_ID_FRAC,
                               self.TASK_ID_FRAC,
                               self.DATE_FRAC).delete()
        self.session.commit()
        self.session.close()
        super(TestTaskInstanceStateView, self).tearDown()

    def test_set_tasks_states(self):
        ## By inspecting the requests using a browser, we know the view's
        ## expected data format. We try to set a different state to the task
        ## instances.
        post_data = {
            'action': 'set_success',
            'rowid': [','.join([self.TASK_ID, self.DAG_ID, str(self.DATE)]),
                      ','.join([self.TASK_ID_FRAC, self.DAG_ID_FRAC,
                                str(self.DATE_FRAC)])]
        }
        response = self.app.post(
            self.TI_ENDPOINT,
            data=post_data,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        ## Checking if the task instances were updated.
        ti = self.findTaskInstances(
            self.session, self.DAG_ID, self.TASK_ID, self.DATE).one().state
        ti_frac = self.findTaskInstances(
            self.session, self.DAG_ID_FRAC, self.TASK_ID_FRAC, self.DATE_FRAC
        ).one().state
        self.assertEqual(ti, State.SUCCESS)
        self.assertEqual(ti_frac, State.SUCCESS)

    def test_action_clear(self):
        post_data = {
            'action': 'clear',
            'rowid': [','.join([self.TASK_ID, self.DAG_ID, str(self.DATE)]),
                      ','.join([self.TASK_ID_FRAC, self.DAG_ID_FRAC,
                                str(self.DATE_FRAC)])]
        }
        response = self.app.post(
            self.TI_ENDPOINT,
            data=post_data,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        ti = self.findTaskInstances(
            self.session, self.DAG_ID, self.TASK_ID, self.DATE).one().state
        ti_frac = self.findTaskInstances(
            self.session, self.DAG_ID_FRAC, self.TASK_ID_FRAC, self.DATE_FRAC
        ).one().state
        self.assertEqual(ti, None)
        self.assertEqual(ti_frac, None)


if __name__ == '__main__':
    unittest.main()
