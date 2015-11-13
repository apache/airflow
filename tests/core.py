from datetime import datetime, time, timedelta
import doctest
import os
from time import sleep
import unittest

from airflow import configuration
configuration.test_mode()
from airflow import jobs, models, DAG, utils, operators, hooks, macros
from airflow.hooks import BaseHook
from airflow.bin import cli
from airflow.www import app as application
from airflow.settings import Session
from lxml import html

NUM_EXAMPLE_DAGS = 7
DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_tests'
configuration.test_mode()

try:
    import cPickle as pickle
except ImportError:
    # Python 3
    import pickle


def reset():
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=TEST_DAG_ID)
    tis.delete()
    session.commit()
    session.close()

reset()

class CoreTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag = dag
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')

    def test_confirm_unittest_mod(self):
        assert configuration.get('core', 'unit_test_mode')

    def test_backfill_examples(self):
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        dags = [
            dag for dag in self.dagbag.dags.values()
            if dag.dag_id in ('example_bash_operator',)]
        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
        for dag in dags:
            job = jobs.BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
            job.run()

    def test_pickling(self):
        dp = self.dag.pickle()
        assert self.dag.dag_id == dp.pickle.dag_id

    def test_rich_comparison_ops(self):

        class DAGsubclass(DAG):
            pass

        dag_eq = DAG(TEST_DAG_ID, default_args=self.args)

        dag_diff_load_time = DAG(TEST_DAG_ID, default_args=self.args)
        dag_diff_name = DAG(TEST_DAG_ID + '_neq', default_args=self.args)

        dag_subclass = DAGsubclass(TEST_DAG_ID, default_args=self.args)
        dag_subclass_diff_name = DAGsubclass(
            TEST_DAG_ID + '2', default_args=self.args)

        for d in [dag_eq, dag_diff_name, dag_subclass, dag_subclass_diff_name]:
            d.last_loaded = self.dag.last_loaded

        # test identity equality
        assert self.dag == self.dag

        # test dag (in)equality based on _comps
        assert self.dag == dag_eq
        assert self.dag != dag_diff_name
        assert self.dag != dag_diff_load_time

        # test dag inequality based on type even if _comps happen to match
        assert self.dag != dag_subclass

        # a dag should equal an unpickled version of itself
        assert self.dag == pickle.loads(pickle.dumps(self.dag))

        # dags are ordered based on dag_id no matter what the type is
        assert self.dag < dag_diff_name
        assert not self.dag < dag_diff_load_time
        assert self.dag < dag_subclass_diff_name

        # greater than should have been created automatically by functools
        assert dag_diff_name > self.dag

        # hashes are non-random and match equality
        assert hash(self.dag) == hash(self.dag)
        assert hash(self.dag) == hash(dag_eq)
        assert hash(self.dag) != hash(dag_diff_name)
        assert hash(self.dag) != hash(dag_subclass)

    def test_time_sensor(self):
        t = operators.TimeSensor(
            task_id='time_sensor_check',
            target_time=time(0),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_check_operators(self):

        conn_id = "sqlite_default"

        captainHook = BaseHook.get_hook(conn_id=conn_id)
        captainHook.run("CREATE TABLE operator_test_table (a, b)")
        captainHook.run("insert into operator_test_table values (1,2)")

        t = operators.CheckOperator(
            task_id='check',
            sql="select count(*) from operator_test_table" ,
            conn_id=conn_id,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        t = operators.ValueCheckOperator(
            task_id='value_check',
            pass_value=95,
            tolerance=0.1,
            conn_id=conn_id,
            sql="SELECT 100",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        captainHook.run("drop table operator_test_table")

    def test_clear_api(self):
        task = self.dag_bash.tasks[0]
        task.clear(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
            upstream=True, downstream=True)
        ti = models.TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.are_dependents_done()

    def test_bash_operator(self):
        t = operators.BashOperator(
            task_id='time_sensor_check',
            bash_command="echo success",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_dryrun(self):
        t = operators.BashOperator(
            task_id='time_sensor_check',
            bash_command="echo success",
            dag=self.dag)
        t.dry_run()

    def test_sqlite(self):
        t = operators.SqliteOperator(
            task_id='time_sqlite',
            sql="CREATE TABLE IF NOT EXISTS unitest (dummy VARCHAR(20))",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_timedelta_sensor(self):
        t = operators.TimeDeltaSensor(
            task_id='timedelta_sensor_check',
            delta=timedelta(seconds=2),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_external_task_sensor(self):
        t = operators.ExternalTaskSensor(
            task_id='test_external_task_sensor_check',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_external_task_sensor_delta(self):
        t = operators.ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            execution_delta=timedelta(0),
            allowed_states=['success'],
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_timeout(self):
        t = operators.PythonOperator(
            task_id='test_timeout',
            execution_timeout=timedelta(seconds=1),
            python_callable=lambda: sleep(5),
            dag=self.dag)
        self.assertRaises(
            utils.AirflowTaskTimeout,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_python_op(self):
        def test_py_op(templates_dict, ds, **kwargs):
            if not templates_dict['ds'] == ds:
                raise Exception("failure")
        t = operators.PythonOperator(
            task_id='test_py_op',
            provide_context=True,
            python_callable=test_py_op,
            templates_dict={'ds': "{{ ds }}"},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_complex_template(self):
        class OperatorSubclass(operators.BaseOperator):
            template_fields = ['some_templated_field']
            def __init__(self, some_templated_field, *args, **kwargs):
                super(OperatorSubclass, self).__init__(*args, **kwargs)
                self.some_templated_field = some_templated_field
            def execute(*args, **kwargs):
                pass
        def test_some_templated_field_template_render(context):
            self.assertEqual(context['ti'].task.some_templated_field['bar'][1], context['ds'])
        t = OperatorSubclass(
            task_id='test_complex_template',
            provide_context=True,
            some_templated_field={
                'foo':'123',
                'bar':['baz', '{{ ds }}']
            },
            on_success_callback=test_some_templated_field_template_render,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_import_examples(self):
        self.assertEqual(len(self.dagbag.dags), NUM_EXAMPLE_DAGS)

    def test_local_task_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, force=True)
        job.run()

    def test_scheduler_job(self):
        job = jobs.SchedulerJob(dag_id='example_bash_operator', test_mode=True)
        job.run()

    def test_raw_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(force=True)

    def test_doctests(self):
        modules = [utils, macros]
        for mod in modules:
            failed, tests = doctest.testmod(mod)
            if failed:
                raise Exception("Failed a doctest")


class CliTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        app = application.create_app()
        app.config['TESTING'] = True
        self.parser = cli.get_parser()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)

    def test_cli_list_dags(self):
        args = self.parser.parse_args(['list_dags'])
        cli.list_dags(args)

    def test_cli_list_tasks(self):
        for dag_id in self.dagbag.dags.keys():
            args = self.parser.parse_args(['list_tasks', dag_id])
            cli.list_tasks(args)

        args = self.parser.parse_args([
            'list_tasks', 'example_bash_operator', '--tree'])
        cli.list_tasks(args)

    def test_cli_initdb(self):
        cli.initdb(self.parser.parse_args(['initdb']))

    def test_cli_test(self):
        cli.test(self.parser.parse_args([
            'test', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))
        cli.test(self.parser.parse_args([
            'test', 'example_bash_operator', 'runme_0', '--dry_run',
            DEFAULT_DATE.isoformat()]))

    def test_cli_run(self):
        cli.run(self.parser.parse_args([
            'run', 'example_bash_operator', 'runme_0', '-l',
            DEFAULT_DATE.isoformat()]))

    def test_task_state(self):
        cli.task_state(self.parser.parse_args([
            'task_state', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))

    def test_backfill(self):
        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '-l',
            '-s', DEFAULT_DATE.isoformat()]))


class WebUiTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        configuration.conf.set("webserver", "authenticate", "False")
        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()

    def test_index(self):
        response = self.app.get('/', follow_redirects=True)
        assert "DAGs" in response.data.decode('utf-8')
        assert "example_bash_operator" in response.data.decode('utf-8')

    def test_query(self):
        response = self.app.get('/admin/queryview/')
        assert "Ad Hoc Query" in response.data.decode('utf-8')
        response = self.app.get(
            "/admin/queryview/?"
            "conn_id=airflow_db&"
            "sql=SELECT+COUNT%281%29+as+TEST+FROM+task_instance")
        assert "TEST" in response.data.decode('utf-8')

    def test_health(self):
        response = self.app.get('/health')
        assert 'The server is healthy!' in response.data.decode('utf-8')

    def test_dag_views(self):
        response = self.app.get(
            '/admin/airflow/graph?dag_id=example_bash_operator')
        assert "runme_0" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/tree?num_runs=25&dag_id=example_bash_operator')
        assert "runme_0" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/duration?days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/landing_times?'
            'days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/gantt?dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/code?dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/blocked')
        response = self.app.get(
            '/admin/configurationview/')
        assert "Airflow Configuration" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/rendered?'
            'task_id=runme_1&dag_id=example_bash_operator&'
            'execution_date={}'.format(DEFAULT_DATE_ISO))
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/log?task_id=run_this_last&'
            'dag_id=example_bash_operator&execution_date={}'
            ''.format(DEFAULT_DATE_ISO))
        assert "run_this_last" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/task?'
            'task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}'.format(DEFAULT_DATE_DS))
        assert "Attributes" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/dag_stats')
        assert "example_bash_operator" in response.data.decode('utf-8')
        url = (
            "/admin/airflow/success?task_id=run_this_last&"
            "dag_id=example_bash_operator&upstream=false&downstream=false&"
            "future=false&past=false&execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        assert "Wait a minute" in response.data.decode('utf-8')
        response = self.app.get(url + "&confirmed=true")
        response = self.app.get(
            '/admin/airflow/clear?task_id=run_this_last&'
            'dag_id=example_bash_operator&future=true&past=false&'
            'upstream=true&downstream=false&'
            'execution_date={}&'
            'origin=/admin'.format(DEFAULT_DATE_DS))
        assert "Wait a minute" in response.data.decode('utf-8')
        url = (
            "/admin/airflow/clear?task_id=runme_1&"
            "dag_id=example_bash_operator&future=false&past=false&"
            "upstream=false&downstream=true&"
            "execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        assert "Wait a minute" in response.data.decode('utf-8')
        response = self.app.get(url + "&confirmed=true")
        url = (
            "/admin/airflow/run?task_id=runme_0&"
            "dag_id=example_bash_operator&force=true&deps=true&"
            "execution_date={}&origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        response = self.app.get(
            "/admin/airflow/refresh?dag_id=example_bash_operator")
        response = self.app.get("/admin/airflow/refresh_all")
        response = self.app.get(
            "/admin/airflow/paused?"
            "dag_id=example_python_operator&is_paused=false")

    def test_charts(self):
        session = Session()
        chart_label = "Airflow task instance by type"
        chart = session.query(
            models.Chart).filter(models.Chart.label==chart_label).first()
        chart_id = chart.id
        session.close()
        response = self.app.get(
            '/admin/airflow/chart'
            '?chart_id={}&iteration_no=1'.format(chart_id))
        assert "Airflow task instance by type" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/chart_data'
            '?chart_id={}&iteration_no=1'.format(chart_id))
        assert "example" in response.data.decode('utf-8')

    def tearDown(self):
        pass


class WebLdapAuthTest(unittest.TestCase):

    def setUp(self):
        configuration.conf.set("webserver", "authenticate", "True")
        configuration.conf.set("webserver", "auth_backend", "airflow.contrib.auth.backends.ldap_auth")
        try:
            configuration.conf.add_section("ldap")
        except:
            pass
        configuration.conf.set("ldap", "uri", "ldap://localhost:3890")
        configuration.conf.set("ldap", "user_filter", "objectClass=*")
        configuration.conf.set("ldap", "user_name_attr", "uid")
        configuration.conf.set("ldap", "bind_user", "cn=Manager,dc=example,dc=com")
        configuration.conf.set("ldap", "bind_password", "insecure")
        configuration.conf.set("ldap", "basedn", "dc=example,dc=com")
        configuration.conf.set("ldap", "cacert", "")

        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()

    def get_csrf(self, response):
        tree = html.fromstring(response.data)
        form = tree.find('.//form')

        return form.find('.//input[@name="_csrf_token"]').value

    def login(self, username, password):
        response = self.app.get('/admin/airflow/login')
        csrf_token = self.get_csrf(response)

        return self.app.post('/admin/airflow/login', data=dict(
            username=username,
            password=password,
            csrf_token=csrf_token
        ), follow_redirects=True)

    def logout(self):
        return self.app.get('/admin/airflow/logout', follow_redirects=True)

    def test_login_logout_ldap(self):
        assert configuration.getboolean('webserver', 'authenticate') is True

        response = self.login('user1', 'userx')
        assert 'Incorrect login details' in response.data.decode('utf-8')

        response = self.login('userz', 'user1')
        assert 'Incorrect login details' in response.data.decode('utf-8')

        response = self.login('user1', 'user1')
        assert 'Data Profiling' in response.data.decode('utf-8')

        response = self.logout()
        assert 'form-signin' in response.data.decode('utf-8')

    def test_unauthorized(self):
        response = self.app.get("/admin/airflow/landing_times")
        self.assertEqual(response.status_code, 302)

    def tearDown(self):
        configuration.test_mode()
        configuration.conf.set("webserver", "authenticate", "False")


if 'MySqlOperator' in dir(operators):
    # Only testing if the operator is installed
    class MySqlTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {
                'owner': 'airflow',
                'mysql_conn_id': 'airflow_db',
                'start_date': DEFAULT_DATE
            }
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag

        def mysql_operator_test(self):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            t = operators.MySqlOperator(
                task_id='basic_mysql',
                sql=sql,
                mysql_conn_id='airflow_db',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def mysql_operator_test_multi(self):
            sql = [
                "TRUNCATE TABLE test_airflow",
                "INSERT INTO test_airflow VALUES ('X')",
            ]
            t = operators.MySqlOperator(
                task_id='mysql_operator_test_multi',
                mysql_conn_id='airflow_db',
                sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_mysql_to_mysql(self):
            sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 100;"
            t = operators.GenericTransfer(
                task_id='test_m2m',
                preoperator=[
                    "DROP TABLE IF EXISTS test_mysql_to_mysql",
                    "CREATE TABLE IF NOT EXISTS "
                        "test_mysql_to_mysql LIKE INFORMATION_SCHEMA.TABLES"
                ],
                source_conn_id='airflow_db',
                destination_conn_id='airflow_db',
                destination_table="test_mysql_to_mysql",
                sql=sql,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_sql_sensor(self):
            t = operators.SqlSensor(
                task_id='sql_sensor_check',
                conn_id='mysql_default',
                sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


if 'PostgresOperator' in dir(operators):
    # Only testing if the operator is installed
    class PostgresTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag

        def postgres_operator_test(self):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            t = operators.PostgresOperator(
                task_id='basic_postgres', sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

            autocommitTask = operators.PostgresOperator(
                task_id='basic_postgres_with_autocommit',
                sql=sql,
                dag=self.dag,
                autocommit=True)
            autocommitTask.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                force=True)


class HttpOpSensorTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE_ISO}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_get(self):
        t = operators.SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            data={"client": "ubuntu", "q": "airflow"},
            headers={},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_get_response_check(self):
        t = operators.SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            data={"client": "ubuntu", "q": "airflow"},
            response_check=lambda response: ("airbnb/airflow" in response.text),
            headers={},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_sensor(self):
        sensor = operators.HttpSensor(
            task_id='http_sensor_check',
            conn_id='http_default',
            endpoint='/search',
            params={"client": "ubuntu", "q": "airflow"},
            headers={},
            response_check=lambda response: ("airbnb/airflow" in response.text),
            poke_interval=5,
            timeout=15,
            dag=self.dag)
        sensor.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        os.environ['AIRFLOW_CONN_TEST_URI'] = (
            'postgres://username:password@ec2.compute.com:5432/the_database')
        os.environ['AIRFLOW_CONN_TEST_URI_NO_CREDS'] = (
            'postgres://ec2.compute.com/the_database')

    def tearDown(self):
        env_vars = ['AIRFLOW_CONN_TEST_URI', 'AIRFLOW_CONN_AIRFLOW_DB']
        for ev in env_vars:
            if ev in os.environ:
                del os.environ[ev]

    def test_using_env_var(self):
        c = hooks.SqliteHook.get_connection(conn_id='test_uri')
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login == 'username'
        assert c.password == 'password'
        assert c.port == 5432

    def test_using_unix_socket_env_var(self):
        c = hooks.SqliteHook.get_connection(conn_id='test_uri_no_creds')
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login is None
        assert c.password is None
        assert c.port is None

    def test_param_setup(self):
        c = models.Connection(conn_id='local_mysql', conn_type='mysql',
                              host='localhost', login='airflow',
                              password='airflow', schema='airflow')
        assert c.host == 'localhost'
        assert c.schema == 'airflow'
        assert c.login == 'airflow'
        assert c.password == 'airflow'
        assert c.port is None

    def test_env_var_priority(self):
        c = hooks.SqliteHook.get_connection(conn_id='airflow_db')
        assert c.host != 'ec2.compute.com'

        os.environ['AIRFLOW_CONN_AIRFLOW_DB'] = \
            'postgres://username:password@ec2.compute.com:5432/the_database'
        c = hooks.SqliteHook.get_connection(conn_id='airflow_db')
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login == 'username'
        assert c.password == 'password'
        assert c.port == 5432
        del os.environ['AIRFLOW_CONN_AIRFLOW_DB']


@unittest.skipUnless("S3Hook" in dir(hooks),
                     "Skipping test because S3Hook is not installed")
class S3HookTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        self.s3_test_url = "s3://test/this/is/not/a-real-key.txt"

    def test_parse_s3_url(self):
        parsed = hooks.S3Hook.parse_s3_url(self.s3_test_url)
        self.assertEqual(parsed,
                         ("test", "this/is/not/a-real-key.txt"),
                         "Incorrect parsing of the s3 url")


if 'AIRFLOW_RUNALL_TESTS' in os.environ:


    class TransferTests(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {'owner': 'airflow', 'start_date': DEFAULT_DATE_ISO}
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag

        def test_clear(self):
            self.dag.clear(start_date=DEFAULT_DATE, end_date=datetime.now())

        def test_mysql_to_hive(self):
            sql = "SELECT * FROM task_instance LIMIT 1000;"
            t = operators.MySqlToHiveTransfer(
                task_id='test_m2h',
                mysql_conn_id='airflow_db',
                sql=sql,
                hive_table='airflow.test_mysql_to_hive',
                recreate=True,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_mysql_to_hive_partition(self):
            sql = "SELECT * FROM task_instance LIMIT 1000;"
            t = operators.MySqlToHiveTransfer(
                task_id='test_m2h',
                mysql_conn_id='airflow_db',
                sql=sql,
                hive_table='airflow.test_mysql_to_hive_part',
                partition={'ds': DEFAULT_DATE_DS},
                recreate=False,
                create=True,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    class HivePrestoTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag
            self.hql = """
            USE airflow;
            DROP TABLE IF EXISTS static_babynames_partitioned;
            CREATE TABLE IF NOT EXISTS static_babynames_partitioned (
                state string,
                year string,
                name string,
                gender string,
                num int)
            PARTITIONED BY (ds string);
            INSERT OVERWRITE TABLE static_babynames_partitioned
                PARTITION(ds='{{ ds }}')
            SELECT state, year, name, gender, num FROM static_babynames;
            """

        def test_hive(self):
            t = operators.HiveOperator(
                task_id='basic_hql', hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_dryrun(self):
            t = operators.HiveOperator(
                task_id='basic_hql', hql=self.hql, dag=self.dag)
            t.dry_run()

        def test_beeline(self):
            t = operators.HiveOperator(
                task_id='beeline_hql', hive_cli_conn_id='beeline_default',
                hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_presto(self):
            sql = """
            SELECT count(1) FROM airflow.static_babynames_partitioned;
            """
            t = operators.PrestoCheckOperator(
                task_id='presto_check', sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hdfs_sensor(self):
            t = operators.HdfsSensor(
                task_id='hdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_webhdfs_sensor(self):
            t = operators.WebHdfsSensor(
                task_id='webhdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                timeout=120,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_sql_sensor(self):
            t = operators.SqlSensor(
                task_id='hdfs_sensor_check',
                conn_id='presto_default',
                sql="SELECT 'x' FROM airflow.static_babynames LIMIT 1;",
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_stats(self):
            t = operators.HiveStatsCollectionOperator(
                task_id='hive_stats_check',
                table="airflow.static_babynames_partitioned",
                partition={'ds': DEFAULT_DATE_DS},
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_partition_sensor(self):
            t = operators.HivePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_metastore_sql_sensor(self):
            t = operators.MetastorePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                partition_name='ds={}'.format(DEFAULT_DATE_DS),
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive2samba(self):
            if 'Hive2SambaOperator' in dir(operators):
                t = operators.Hive2SambaOperator(
                    task_id='hive2samba_check',
                    samba_conn_id='tableau_samba',
                    hql="SELECT * FROM airflow.static_babynames LIMIT 10000",
                    destination_filepath='test_airflow.csv',
                    dag=self.dag)
                t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_to_mysql(self):
            t = operators.HiveToMySqlTransfer(
                mysql_conn_id='airflow_db',
                task_id='hive_to_mysql_check',
                create=True,
                sql="""
                SELECT name
                FROM airflow.static_babynames
                LIMIT 100
                """,
                mysql_table='test_static_babynames',
                mysql_preoperator=[
                    'DROP TABLE IF EXISTS test_static_babynames;',
                    'CREATE TABLE test_static_babynames (name VARCHAR(500))',
                ],
                dag=self.dag)
            t.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


if __name__ == '__main__':
    unittest.main()
