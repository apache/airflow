from datetime import datetime, time, timedelta
from time import sleep
import unittest
from airflow import configuration
configuration.test_mode()
from airflow import jobs, models, DAG, executors, utils, operators
from airflow.www.app import app
from airflow import utils

NUM_EXAMPLE_DAGS = 5
DEV_NULL = '/dev/null'
LOCAL_EXECUTOR = executors.LocalExecutor()
DEFAULT_DATE = datetime(2015, 1, 1)
configuration.test_mode()


class TransferTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
        dag = DAG('hive_test', default_args=args)
        self.dag = dag

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
            partition={'ds': '2015-01-02'},
            recreate=False,
            create=True,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


class HivePrestoTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
        dag = DAG('hive_test', default_args=args)
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
            filepath='/user/hive/warehouse/airflow.db/static_babynames',
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
            partition={'ds': '2015-01-01'},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_hive_partition_sensor(self):
        t = operators.HivePartitionSensor(
            task_id='hive_partition_check',
            table='airflow.static_babynames_partitioned',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_hive2samba(self):
        t = operators.Hive2SambaOperator(
            task_id='hive2samba_check',
            samba_conn_id='tableau_samba',
            hql="SELECT * FROM airflow.static_babynames LIMIT 10000",
            destination_filepath='test_airflow.csv',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_hive_to_mysql(self):
        t = operators.HiveToMySqlTransfer(
            task_id='hive_to_mysql_check',
            sql="""
            SELECT name, count(*) as ccount
            FROM airflow.static_babynames
            GROUP BY name
            """,
            mysql_table='test_static_babynames',
            mysql_preoperator='TRUNCATE TABLE test_static_babynames;',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


class CoreTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        utils.initdb()
        args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
        dag = DAG('core_test', default_args=args)
        self.dag = dag
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')

    def test_confirm_unittest_mod(self):
        assert configuration.conf.get('core', 'unit_test_mode')

    def test_time_sensor(self):
        t = operators.TimeSensor(
            task_id='time_sensor_check',
            target_time=time(0),
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
            external_dag_id='core_test',
            external_task_id='time_sensor_check',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_external_task_sensor_delta(self):
        t = operators.ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id='core_test',
            external_task_id='time_sensor_check',
            execution_delta=timedelta(0),
            allowed_states=['success'],
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_timeout(self):
        t = operators.PythonOperator(
            task_id='test_timeout',
            execution_timeout=timedelta(seconds=2),
            python_callable=lambda: sleep(10),
            dag=self.dag)
        self.assertRaises(
            utils.AirflowTaskTimeout,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

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

    def test_local_backfill_job(self):
        self.dag_bash.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        job = jobs.BackfillJob(
            dag=self.dag_bash,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        job.run()

    def test_raw_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(force=True)


class WebUiTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        app.config['TESTING'] = True
        self.app = app.test_client()

    def test_index(self):
        response = self.app.get('/', follow_redirects=True)
        assert "DAGs" in response.data
        assert "example_bash_operator" in response.data

    def test_query(self):
        response = self.app.get('/admin/queryview/')
        assert "Ad Hoc Query" in response.data
        response = self.app.get(
            "/admin/queryview/?"
            "conn_id=presto_default&"
            "sql=SELECT+COUNT%281%29+FROM+airflow.static_babynames")
        assert "Ad Hoc Query" in response.data

    def test_health(self):
        response = self.app.get('/health')
        assert 'The server is healthy!' in response.data

    def test_dag_views(self):
        response = self.app.get(
            '/admin/airflow/graph?dag_id=example_bash_operator')
        assert "runme_0" in response.data
        response = self.app.get(
            '/admin/airflow/tree?num_runs=25&dag_id=example_bash_operator')
        assert "runme_0" in response.data
        response = self.app.get(
            '/admin/airflow/duration?days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/landing_times?'
            'days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/gantt?dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/code?dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data
        response = self.app.get(
            '/admin/configurationview/')
        assert "Airflow Configuration" in response.data
        response = self.app.get(
            '/admin/airflow/rendered?'
            'task_id=runme_1&dag_id=example_bash_operator&'
            'execution_date=2015-01-07T00:00:00')
        assert "example_bash_operator__runme_1__20150107" in response.data
        response = self.app.get(
            '/admin/airflow/log?task_id=run_this_last&'
            'dag_id=example_bash_operator&execution_date=2015-01-01T00:00:00')
        assert "run_this_last" in response.data
        response = self.app.get(
            '/admin/airflow/task?'
            'task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date=2015-01-01')
        assert "Attributes" in response.data
        response = self.app.get(
            '/admin/airflow/dag_stats')
        assert "example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/action?action=clear&task_id=run_this_last&'
            'dag_id=example_bash_operator&future=true&past=false&'
            'upstream=true&downstream=false&'
            'execution_date=2015-01-01T00:00:00&'
            'origin=http%3A%2F%2Fjn8.brain.musta.ch%3A8080%2Fadmin%2Fairflow'
            '%2Ftree%3Fnum_runs%3D65%26dag_id%3Dexample_bash_operator')
        assert "Wait a minute" in response.data
        response = self.app.get(
            '/admin/airflow/action?action=clear&task_id=run_this_last&'
            'dag_id=example_bash_operator&future=true&past=false&'
            'upstream=true&downstream=false&'
            'execution_date=2015-01-01T00:00:00&confirmed=true&'
            'origin=http%3A%2F%2Fjn8.brain.musta.ch%3A8080%2Fadmin%2Fairflow'
            '%2Ftree%3Fnum_runs%3D65%26dag_id%3Dexample_bash_operator')

    def test_charts(self):
        response = self.app.get(
            '/admin/airflow/chart?chart_id=1&iteration_no=1')
        assert "Most Popular" in response.data
        response = self.app.get(
            '/admin/airflow/chart_data?chart_id=1&iteration_no=1')
        assert "Michael" in response.data

    def tearDown(self):
        pass

if 'MySqlOperator' in dir(operators):
    # Only testing if the operator is installed
    class MySqlTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            utils.initdb()
            args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
            dag = DAG('hive_test', default_args=args)
            self.dag = dag

        def mysql_operator_test(self):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            t = operators.MySqlOperator(
                task_id='basic_mysql', sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

if 'PostgresOperator' in dir(operators):
    # Only testing if the operator is installed
    class PostgresTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            utils.initdb()
            args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
            dag = DAG('hive_test', default_args=args)
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
        utils.initdb()
        args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
        dag = DAG('http_test', default_args=args)
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

    def test_sensor_timeout(self):
        sensor = operators.HttpSensor(
            task_id='http_sensor_check',
            conn_id='http_default',
            endpoint='/search',
            params={"client": "ubuntu", "q": "airflow"},
            headers={},
            response_check=lambda response: ("dingdong" in response.text),
            poke_interval=2,
            timeout=5,
            dag=self.dag)
        with self.assertRaises(utils.AirflowSensorTimeout):
            sensor.run(
                start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

if __name__ == '__main__':
    unittest.main()
