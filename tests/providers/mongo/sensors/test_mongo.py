from __future__ import annotations

from unittest.mock import MagicMock, patch

from airflow.models.dag import DAG
from airflow.providers.mongo.sensors.mongo import MongoSensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

class TestMongoSensor:
    def setup_method(self, method):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

        self.mock_context = MagicMock()

    @patch("airflow.providers.mongo.hooks.mongo.MongoHook.find")
    def test_execute_operator(self, mock_mongo_find):
        mock_mongo_find.return_value = {"test_key": "test"} 
        sensor = MongoSensor(
            collection="coll",
            query={"test_key": "test"},
            mongo_conn_id="mongo_default",
            mongo_db="test_db",
            task_id="test_task",
            dag=self.dag
        )
        result = sensor.poke(self.mock_context)

        assert result is True

        # assert result is True
        mock_mongo_find.assert_called_once_with(
            "coll", 
            {"test_key": "test"}, 
            mongo_db="test_db", 
            find_one=True
        )