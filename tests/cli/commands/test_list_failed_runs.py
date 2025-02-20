import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, Mock
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.cli.commands.dag_command import list_failed_runs
from io import StringIO
from argparse import Namespace

class TestListFailedRuns(unittest.TestCase):

    @patch('airflow.utils.timezone.utcnow')
    @patch('airflow.models.dagrun.DagRun.find')
    def test_list_failed_runs_simplified(self, mock_dag_run_find, mock_utcnow):
        fixed_now = datetime(2025, 2, 19, 12, 0, 0, tzinfo=timezone.utc)
        mock_utcnow.return_value = fixed_now
        since_24h_ago = fixed_now - timedelta(hours=24)

        mock_dag_run1 = Mock(dag_id="dag1", execution_date=fixed_now - timedelta(hours=1), state="failed")
        mock_dag_run3 = Mock(dag_id="dag2", execution_date=fixed_now - timedelta(hours=2), state="failed")
        mock_dag_run5 = Mock(dag_id="dag4", execution_date=fixed_now - timedelta(minutes=10), state="failed")

        mock_dag_run_find.return_value = [mock_dag_run1, mock_dag_run3, mock_dag_run5]

        mock_args = Namespace(verbose=False, since=fixed_now - timedelta(hours=24))

        print(f"Mock args: {mock_args}") 

        with patch('sys.stdout', new=StringIO()) as fake_out:
            list_failed_runs(mock_args)  
            output = fake_out.getvalue()

        assert "dag1" in output, f"Expected 'dag1' in output, but got: {output}"

