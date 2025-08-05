import pytest
import json
from unittest.mock import patch, MagicMock

import requests
from airflow.models import Connection
from airflow.exceptions import AirflowException

from airflow_seatunnel_provider.sensors.seatunnel_sensor import SeaTunnelJobSensor


class TestSeaTunnelJobSensorIntegration:
    """Integration tests for SeaTunnelJobSensor"""

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

    def test_sensor_initialization(self):
        """Test sensor initialization"""
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test",
            poke_interval=30,
            timeout=600
        )
        
        assert sensor.task_id == "test_sensor"
        assert sensor.job_id == "test_job_123"
        assert sensor.target_states == ['FINISHED']
        assert sensor.seatunnel_conn_id == "seatunnel_test"

    def test_sensor_initialization_default_target_states(self):
        """Test sensor initialization with default target states"""
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            seatunnel_conn_id="seatunnel_test"
        )
        
        assert sensor.target_states == ['FINISHED']

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    def test_poke_non_zeta_engine_raises_error(self, mock_get_connection, seatunnel_connection):
        """Test that sensor raises error for non-zeta engines"""
        mock_get_connection.return_value = seatunnel_connection
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            seatunnel_conn_id="seatunnel_test"
        )
        
        # Patch the hook to use a different engine
        with patch('airflow_seatunnel_provider.sensors.seatunnel_sensor.SeaTunnelHook') as mock_hook_class:
            mock_hook = MagicMock()
            mock_hook.engine = "flink"
            mock_hook_class.return_value = mock_hook
            
            context = {"task_instance": MagicMock()}
            
            with pytest.raises(ValueError, match="SeaTunnelJobSensor only works with the 'zeta' engine"):
                sensor.poke(context)

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_job_finished_success(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test successful job completion detection"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobId": "test_job_123",
            "jobStatus": "FINISHED",
            "jobName": "test_job"
        }
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = sensor.poke(context)
        
        assert result is True
        mock_get.assert_called_once_with(
            "http://localhost:8083/job-info/test_job_123",
            timeout=10
        )

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_job_running(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test job still running detection"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock API response with running status
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobId": "test_job_123",
            "jobStatus": "RUNNING",
            "jobName": "test_job"
        }
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = sensor.poke(context)
        
        assert result is False

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_job_failed(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test job failure detection"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock API response with failed status
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobId": "test_job_123",
            "jobStatus": "FAILED",
            "jobName": "test_job"
        }
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        
        with pytest.raises(Exception, match="SeaTunnel job test_job_123 is in FAILED state"):
            sensor.poke(context)

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_job_canceled(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test job cancellation detection"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock API response with canceled status
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobId": "test_job_123",
            "jobStatus": "CANCELED",
            "jobName": "test_job"
        }
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        
        with pytest.raises(Exception, match="SeaTunnel job test_job_123 is in CANCELED state"):
            sensor.poke(context)

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_multiple_target_states(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test sensor with multiple target states"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock API response with completed status
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobId": "test_job_123",
            "jobStatus": "COMPLETED",
            "jobName": "test_job"
        }
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED', 'COMPLETED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = sensor.poke(context)
        
        assert result is True

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_api_404_error(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test handling of 404 API errors"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock 404 response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Job not found"
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = sensor.poke(context)
        
        assert result is False

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_request_exception(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test handling of request exceptions"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock request exception
        mock_get.side_effect = requests.RequestException("Connection error")
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = sensor.poke(context)
        
        assert result is False

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_invalid_json_response(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test handling of invalid JSON responses"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock response with invalid JSON
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_response.text = "Invalid JSON response"
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = sensor.poke(context)
        
        assert result is False

    @patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection')
    @patch('requests.get')
    def test_poke_missing_job_status_field(self, mock_get, mock_get_connection, seatunnel_connection):
        """Test handling of responses missing jobStatus field"""
        mock_get_connection.return_value = seatunnel_connection
        
        # Mock response without jobStatus field
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobId": "test_job_123",
            "jobName": "test_job"
            # Missing jobStatus field
        }
        mock_get.return_value = mock_response
        
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test"
        )
        
        context = {"task_instance": MagicMock()}
        result = sensor.poke(context)
        
        assert result is False

    def test_template_fields(self):
        """Test that template fields are correctly defined"""
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            seatunnel_conn_id="seatunnel_test"
        )
        
        expected_template_fields = ('job_id', 'target_states')
        assert sensor.template_fields == expected_template_fields

    def test_ui_color(self):
        """Test that UI color is correctly set"""
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            seatunnel_conn_id="seatunnel_test"
        )
        
        assert sensor.ui_color == '#1CB8FF'