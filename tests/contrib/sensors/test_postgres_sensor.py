import unittest

from airflow import DAG, configuration
from airflow.contrib.sensors.postgres_sensor import PostgresTableSensor
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime


class PostgresSensorTests(unittest.TestCase):
    _config = {
        'table': 'postgres_test_table',
        'conn_id': 'postgres_default',
        'timeout': 5
    }

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime(2017, 10, 20)
        }
        self.dag = DAG('test_dag_id', default_args=args)

        sql = """
        CREATE TABLE IF NOT EXISTS postgres_test_table (
            dummy VARCHAR(50)
        );
        """
        create_postgres_table_operator = PostgresOperator(
            task_id='create_postgres_table_operator',
            dag=self.dag,
            sql=sql
        )
        create_postgres_table_operator.run(ignore_ti_state=True)

    def test_init(self):
        sensor = PostgresTableSensor(
            task_id='postgres_table_sensor_test',
            dag=self.dag,
            **self._config
        )

        self.assertEqual(sensor.table, self._config['table'])
        self.assertEqual(sensor.conn_id, self._config['conn_id'])
        self.assertEqual(sensor.timeout, self._config['timeout'])

    def test_table_exists(self):
        sensor = PostgresTableSensor(
            task_id='postgres_table_sensor_test',
            dag=self.dag,
            **self._config
        )
        table_exists = sensor.poke(None)
        self.assertTrue(table_exists)

    def test_table_doesnt_exists(self):
        sensor = PostgresTableSensor(
            task_id='postgres_table_sensor_test',
            dag=self.dag,
            timeout=self._config['timeout'],
            table='test_airflow_2'
        )
        table_exists = sensor.poke(None)
        self.assertFalse(table_exists)

if __name__ == '__main__':
    unittest.main()
