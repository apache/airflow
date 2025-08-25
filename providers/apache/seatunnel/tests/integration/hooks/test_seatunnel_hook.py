import os
import pytest
import tempfile
from unittest.mock import patch, MagicMock

from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_seatunnel_provider.hooks.seatunnel_hook import SeaTunnelHook


class TestSeaTunnelHookIntegration:
    """Integration tests for SeaTunnelHook"""

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

    @pytest.fixture
    def hook(self, seatunnel_connection):
        """Create a SeaTunnelHook instance for testing"""
        with patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection') as mock_get_conn:
            mock_get_conn.return_value = seatunnel_connection
            return SeaTunnelHook(seatunnel_conn_id="seatunnel_test")

    def test_hook_initialization(self, hook):
        """Test that the hook initializes correctly"""
        assert hook.seatunnel_conn_id == "seatunnel_test"
        assert hook.engine == "zeta"
        assert hook.host == "localhost"
        assert hook.port == 8083
        assert hook.seatunnel_home == "/opt/seatunnel"

    def test_hook_initialization_without_seatunnel_home(self):
        """Test that hook raises exception when seatunnel_home is not provided"""
        connection = Connection(
            conn_id="seatunnel_test",
            conn_type="seatunnel",
            host="localhost",
            port=8083,
            extra='{}'
        )
        
        with patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection') as mock_get_conn:
            mock_get_conn.return_value = connection
            with pytest.raises(AirflowException, match="SeaTunnel home directory must be specified"):
                SeaTunnelHook(seatunnel_conn_id="seatunnel_test")

    def test_create_temp_config(self, hook):
        """Test creating temporary configuration file"""
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
        
        temp_file_path = hook.create_temp_config(config_content)
        
        try:
            assert os.path.exists(temp_file_path)
            assert temp_file_path.endswith('.conf')
            
            with open(temp_file_path, 'r') as f:
                content = f.read()
                assert "FakeSource" in content
                assert "Console" in content
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    def test_unsupported_engine(self, hook):
        """Test that unsupported engine raises exception"""
        with tempfile.NamedTemporaryFile(suffix='.conf', delete=False) as temp_file:
            temp_file.write(b"test config")
            temp_file_path = temp_file.name
        
        try:
            with pytest.raises(AirflowException, match="Unsupported engine: invalid"):
                hook.run_job(config_file=temp_file_path, engine="invalid")
        finally:
            os.unlink(temp_file_path)

    @patch('subprocess.Popen')
    @patch('os.path.isfile')
    def test_run_job_zeta_engine_success(self, mock_isfile, mock_popen, hook):
        """Test running a job with Zeta engine successfully"""
        # Mock file existence
        mock_isfile.return_value = True
        
        # Mock successful process
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = iter(["Job started successfully\n", "Job completed\n"])
        mock_popen.return_value = mock_process
        
        with tempfile.NamedTemporaryFile(suffix='.conf', delete=False) as temp_file:
            temp_file.write(b"test config")
            temp_file_path = temp_file.name
        
        try:
            output = hook.run_job(config_file=temp_file_path, engine="zeta")
            assert "Job started successfully" in output
            assert "Job completed" in output
            
            # Verify the command was called correctly
            expected_cmd = ["/opt/seatunnel/bin/seatunnel.sh", "--config", temp_file_path]
            mock_popen.assert_called_once()
            args, kwargs = mock_popen.call_args
            assert args[0] == expected_cmd
        finally:
            os.unlink(temp_file_path)

    @patch('subprocess.Popen')
    @patch('os.path.isfile')
    def test_run_job_flink_engine_success(self, mock_isfile, mock_popen, hook):
        """Test running a job with Flink engine successfully"""
        # Mock file existence
        mock_isfile.return_value = True
        
        # Mock successful process
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = iter(["Flink job started\n", "Job completed\n"])
        mock_popen.return_value = mock_process
        
        with tempfile.NamedTemporaryFile(suffix='.conf', delete=False) as temp_file:
            temp_file.write(b"test config")
            temp_file_path = temp_file.name
        
        try:
            output = hook.run_job(config_file=temp_file_path, engine="flink")
            assert "Flink job started" in output
            
            # Verify the command was called correctly for Flink
            expected_cmd = ["/opt/seatunnel/bin/start-seatunnel-flink-connector-v2.sh", "--config", temp_file_path, "--deploy", "local"]
            mock_popen.assert_called_once()
            args, kwargs = mock_popen.call_args
            assert args[0] == expected_cmd
        finally:
            os.unlink(temp_file_path)

    @patch('subprocess.Popen')
    @patch('os.path.isfile')
    def test_run_job_failure(self, mock_isfile, mock_popen, hook):
        """Test handling of job execution failure"""
        # Mock file existence
        mock_isfile.return_value = True
        
        # Mock failed process
        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_process.stdout = iter(["Error occurred\n"])
        mock_popen.return_value = mock_process
        
        with tempfile.NamedTemporaryFile(suffix='.conf', delete=False) as temp_file:
            temp_file.write(b"test config")
            temp_file_path = temp_file.name
        
        try:
            with pytest.raises(AirflowException, match="SeaTunnel job failed with return code 1"):
                hook.run_job(config_file=temp_file_path)
        finally:
            os.unlink(temp_file_path)

    def test_run_job_config_file_not_found(self, hook):
        """Test handling of missing configuration file"""
        with pytest.raises(AirflowException, match="Config file not found"):
            hook.run_job(config_file="/nonexistent/config.conf")

    @patch('os.path.isfile')
    def test_run_job_script_not_found(self, mock_isfile, hook):
        """Test handling of missing SeaTunnel script"""
        # Config file exists but script doesn't
        mock_isfile.side_effect = lambda path: path.endswith('.conf')
        
        with tempfile.NamedTemporaryFile(suffix='.conf', delete=False) as temp_file:
            temp_file.write(b"test config")
            temp_file_path = temp_file.name
        
        try:
            with pytest.raises(AirflowException, match="SeaTunnel script not found"):
                hook.run_job(config_file=temp_file_path)
        finally:
            os.unlink(temp_file_path)

    def test_get_ui_field_behaviour(self):
        """Test UI field behavior configuration"""
        behaviour = SeaTunnelHook.get_ui_field_behaviour()
        
        assert "hidden_fields" in behaviour
        assert "login" in behaviour["hidden_fields"]
        assert "password" in behaviour["hidden_fields"]
        assert "schema" in behaviour["hidden_fields"]
        
        assert "relabeling" in behaviour
        assert behaviour["relabeling"]["host"] == "SeaTunnel Host"
        assert behaviour["relabeling"]["port"] == "SeaTunnel Port"
        
        assert "placeholders" in behaviour
        assert behaviour["placeholders"]["host"] == "localhost"
        assert behaviour["placeholders"]["port"] == "8083"