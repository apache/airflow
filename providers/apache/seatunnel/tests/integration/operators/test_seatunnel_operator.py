import os
import pytest
import tempfile
from unittest.mock import patch, MagicMock

from airflow.models import Connection
from airflow.exceptions import AirflowException

from airflow_seatunnel_provider.operators.seatunnel_operator import SeaTunnelOperator


class TestSeaTunnelOperatorIntegration:
    """Integration tests for SeaTunnelOperator"""

    @pytest.fixture
    def seatunnel_connection(self):
        """Create a test connection for SeaTunnel"""
        return Connection(
            conn_id="seatunnel_test",
            conn_type="seatunnel",
            host="localhost",
            port=8083,
            extra='{"seatunnel_home": "/opt/seatunnel"}'
        )

    def test_operator_initialization_with_config_file(self):
        """Test operator initialization with config file"""
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_file="/path/to/config.conf",
            engine="zeta",
            seatunnel_conn_id="seatunnel_test"
        )
        
        assert operator.task_id == "test_task"
        assert operator.config_file == "/path/to/config.conf"
        assert operator.config_content is None
        assert operator.engine == "zeta"
        assert operator.seatunnel_conn_id == "seatunnel_test"

    def test_operator_initialization_with_config_content(self):
        """Test operator initialization with config content"""
        config_content = """
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
  }
}

sink {
  Console {
    plugin_input = "fake"
  }
}
        """
        
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_content=config_content,
            engine="flink",
            seatunnel_conn_id="seatunnel_test"
        )
        
        assert operator.task_id == "test_task"
        assert operator.config_file is None
        assert operator.config_content == config_content
        assert operator.engine == "flink"

    def test_operator_initialization_invalid_config(self):
        """Test operator initialization with invalid configuration"""
        with pytest.raises(ValueError, match="Either config_file or config_content must be provided"):
            SeaTunnelOperator(
                task_id="test_task",
                seatunnel_conn_id="seatunnel_test"
            )
        
        with pytest.raises(ValueError, match="Either config_file or config_content must be provided"):
            SeaTunnelOperator(
                task_id="test_task",
                config_file="/path/to/config.conf",
                config_content="some content",
                seatunnel_conn_id="seatunnel_test"
            )

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.run_job')
    def test_execute_with_config_file(self, mock_run_job, mock_get_connection, seatunnel_connection):
        """Test executing operator with config file"""
        mock_get_connection.return_value = seatunnel_connection
        mock_run_job.return_value = "Job completed successfully"
        
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_file="/path/to/config.conf",
            engine="zeta",
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = operator.execute(context)
        
        assert result == "Job completed successfully"
        mock_run_job.assert_called_once_with(config_file="/path/to/config.conf", engine="zeta")

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.run_job')
    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.create_temp_config')
    def test_execute_with_config_content(self, mock_create_temp, mock_run_job, mock_get_connection, seatunnel_connection):
        """Test executing operator with config content"""
        mock_get_connection.return_value = seatunnel_connection
        mock_run_job.return_value = "Job completed successfully"
        
        # Create a real temporary file for testing
        with tempfile.NamedTemporaryFile(suffix='.conf', delete=False) as temp_file:
            temp_file_path = temp_file.name
        
        mock_create_temp.return_value = temp_file_path
        
        config_content = """
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
  }
}

sink {
  Console {
    plugin_input = "fake"
  }
}
        """
        
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_content=config_content,
            engine="spark",
            seatunnel_conn_id="seatunnel_test"
        )
        
        try:
            context = {"task_instance": MagicMock()}
            result = operator.execute(context)
            
            assert result == "Job completed successfully"
            mock_create_temp.assert_called_once_with(config_content)
            mock_run_job.assert_called_once_with(config_file=temp_file_path, engine="spark")
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.run_job')
    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.create_temp_config')
    @patch('os.path.exists')
    @patch('os.unlink')
    def test_execute_cleanup_temp_file(self, mock_unlink, mock_exists, mock_create_temp, mock_run_job, mock_get_connection, seatunnel_connection):
        """Test that temporary files are cleaned up after execution"""
        mock_get_connection.return_value = seatunnel_connection
        mock_run_job.return_value = "Job completed successfully"
        mock_create_temp.return_value = "/tmp/test_config.conf"
        mock_exists.return_value = True
        
        config_content = "test config content"
        
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_content=config_content,
            engine="zeta",
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        operator.execute(context)
        
        # Verify that the temporary file was cleaned up
        mock_unlink.assert_called_once_with("/tmp/test_config.conf")

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.run_job')
    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.create_temp_config')
    @patch('os.path.exists')
    @patch('os.unlink')
    def test_execute_cleanup_temp_file_on_exception(self, mock_unlink, mock_exists, mock_create_temp, mock_run_job, mock_get_connection, seatunnel_connection):
        """Test that temporary files are cleaned up even when execution fails"""
        mock_get_connection.return_value = seatunnel_connection
        mock_run_job.side_effect = AirflowException("Job failed")
        mock_create_temp.return_value = "/tmp/test_config.conf"
        mock_exists.return_value = True
        
        config_content = "test config content"
        
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_content=config_content,
            engine="zeta",
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        
        with pytest.raises(AirflowException, match="Job failed"):
            operator.execute(context)
        
        # Verify that the temporary file was cleaned up even after exception
        mock_unlink.assert_called_once_with("/tmp/test_config.conf")

    def test_template_fields(self):
        """Test that template fields are correctly defined"""
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_file="/path/to/config.conf",
            seatunnel_conn_id="seatunnel_test"
        )
        
        expected_template_fields = ('config_file', 'config_content', 'engine')
        assert operator.template_fields == expected_template_fields

    def test_template_ext(self):
        """Test that template extensions are correctly defined"""
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_file="/path/to/config.conf",
            seatunnel_conn_id="seatunnel_test"
        )
        
        expected_template_ext = ('.conf', '.hocon')
        assert operator.template_ext == expected_template_ext

    def test_ui_color(self):
        """Test that UI color is correctly set"""
        operator = SeaTunnelOperator(
            task_id="test_task",
            config_file="/path/to/config.conf",
            seatunnel_conn_id="seatunnel_test"
        )
        
        assert operator.ui_color == '#1CB8FF'