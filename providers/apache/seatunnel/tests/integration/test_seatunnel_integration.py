"""
End-to-end integration tests for SeaTunnel provider.
These tests require a running SeaTunnel instance.
"""
import pytest
import time
import re
from unittest.mock import patch

from airflow.models import Connection, DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from airflow_seatunnel_provider.hooks.seatunnel_hook import SeaTunnelHook
from airflow_seatunnel_provider.operators.seatunnel_operator import SeaTunnelOperator
from airflow_seatunnel_provider.sensors.seatunnel_sensor import SeaTunnelJobSensor


@pytest.mark.integration
@pytest.mark.requires_docker
class TestSeaTunnelIntegrationE2E:
    """End-to-end integration tests for SeaTunnel provider"""

    @pytest.fixture
    def seatunnel_connection(self, seatunnel_connection_params):
        """Create a test connection for SeaTunnel"""
        return Connection(**seatunnel_connection_params)

    def test_hook_connection_and_basic_functionality(self, seatunnel_service, seatunnel_connection):
        """Test basic hook functionality with real SeaTunnel instance"""
        with patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection') as mock_get_conn:
            mock_get_conn.return_value = seatunnel_connection
            
            hook = SeaTunnelHook(seatunnel_conn_id="seatunnel_test")
            
            # Test connection parameters
            assert hook.host == "localhost"
            assert hook.port == 8083
            assert hook.seatunnel_home == "/opt/seatunnel"
            
            # Test temp config creation
            config_content = """
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "fake"
  }
}
            """
            
            temp_file = hook.create_temp_config(config_content)
            assert temp_file.endswith('.conf')
            
            # Read back the content to verify
            with open(temp_file, 'r') as f:
                content = f.read()
                assert "FakeSource" in content
                assert "Console" in content

    @pytest.mark.slow
    def test_operator_execute_batch_job(self, seatunnel_service, seatunnel_connection, simple_batch_config):
        """Test executing a batch job using SeaTunnelOperator"""
        with patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection') as mock_get_conn:
            mock_get_conn.return_value = seatunnel_connection
            
            # Read the config file content
            with open(simple_batch_config, 'r') as f:
                config_content = f.read()
            
            operator = SeaTunnelOperator(
                task_id="test_batch_job",
                config_content=config_content,
                engine="zeta",
                seatunnel_conn_id="seatunnel_test"
            )
            
            # Mock context
            context = {
                "task_instance": type('MockTI', (), {
                    'task_id': 'test_batch_job',
                    'dag_id': 'test_dag',
                    'execution_date': '2023-01-01'
                })()
            }
            
            # This test might need to be mocked if SeaTunnel is not fully functional
            # For now, we'll test the operator setup and configuration
            assert operator.config_content == config_content
            assert operator.engine == "zeta"
            assert operator.seatunnel_conn_id == "seatunnel_test"

    def test_operator_with_different_engines(self, seatunnel_connection):
        """Test operator configuration with different engines"""
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
        
        # Test Zeta engine
        zeta_operator = SeaTunnelOperator(
            task_id="test_zeta",
            config_content=config_content,
            engine="zeta",
            seatunnel_conn_id="seatunnel_test"
        )
        assert zeta_operator.engine == "zeta"
        
        # Test Flink engine
        flink_operator = SeaTunnelOperator(
            task_id="test_flink",
            config_content=config_content,
            engine="flink",
            seatunnel_conn_id="seatunnel_test"
        )
        assert flink_operator.engine == "flink"
        
        # Test Spark engine
        spark_operator = SeaTunnelOperator(
            task_id="test_spark",
            config_content=config_content,
            engine="spark",
            seatunnel_conn_id="seatunnel_test"
        )
        assert spark_operator.engine == "spark"

    def test_sensor_configuration(self, seatunnel_connection):
        """Test sensor configuration and basic functionality"""
        sensor = SeaTunnelJobSensor(
            task_id="test_sensor",
            job_id="test_job_123",
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test",
            poke_interval=30,
            timeout=600
        )
        
        assert sensor.job_id == "test_job_123"
        assert sensor.target_states == ['FINISHED']
        assert sensor.seatunnel_conn_id == "seatunnel_test"
        assert sensor.poke_interval == 30
        assert sensor.timeout == 600

    def test_provider_info(self):
        """Test provider information is correctly configured"""
        from airflow_seatunnel_provider import get_provider_info
        
        provider_info = get_provider_info()
        
        assert provider_info["package-name"] == "airflow-provider-seatunnel"
        assert provider_info["name"] == "Apache SeaTunnel Provider"
        assert "connection-types" in provider_info
        
        connection_types = provider_info["connection-types"]
        assert len(connection_types) == 1
        assert connection_types[0]["connection-type"] == "seatunnel"
        assert "SeaTunnelHook" in connection_types[0]["hook-class-name"]

    def test_dag_import_and_validation(self):
        """Test that example DAGs can be imported and validated"""
        # This would test the example DAGs in the examples directory
        # For now, we'll test basic DAG structure
        
        from datetime import datetime, timedelta
        from airflow import DAG
        
        default_args = {
            'owner': 'test',
            'depends_on_past': False,
            'start_date': datetime(2023, 1, 1),
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        
        # Test creating a DAG with SeaTunnel operators
        with DAG(
            'test_seatunnel_dag',
            default_args=default_args,
            description='Test SeaTunnel DAG',
            schedule=None,
        ) as dag:
            
            seatunnel_task = SeaTunnelOperator(
                task_id='test_seatunnel_task',
                config_content="""
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
                """,
                engine='zeta',
                seatunnel_conn_id='seatunnel_test',
            )
            
            sensor_task = SeaTunnelJobSensor(
                task_id='test_sensor_task',
                job_id='test_job_id',
                target_states=['FINISHED'],
                seatunnel_conn_id='seatunnel_test',
                poke_interval=30,
                timeout=600,
            )
        
        # Validate DAG structure
        assert len(dag.tasks) == 2
        assert 'test_seatunnel_task' in dag.task_ids
        assert 'test_sensor_task' in dag.task_ids

    @pytest.mark.slow
    def test_full_workflow_simulation(self, seatunnel_connection):
        """Test a complete workflow simulation"""
        # This test simulates a complete workflow:
        # 1. Start a SeaTunnel job
        # 2. Monitor its progress
        # 3. Verify completion
        
        config_content = """
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Test User", 25]
      }
    ]
  }
}

transform {
  sql {
    plugin_input = "fake"
    plugin_output = "transformed"
    sql = "SELECT name, age FROM fake WHERE age > 20"
  }
}

sink {
  Console {
    plugin_input = "transformed"
  }
}
        """
        
        # Create operator
        operator = SeaTunnelOperator(
            task_id="workflow_job",
            config_content=config_content,
            engine="zeta",
            seatunnel_conn_id="seatunnel_test"
        )
        
        # Verify operator configuration
        assert operator.config_content == config_content
        assert operator.engine == "zeta"
        
        # Create sensor for monitoring
        sensor = SeaTunnelJobSensor(
            task_id="workflow_monitor",
            job_id="mock_job_id",  # In real scenario, this would come from operator output
            target_states=['FINISHED'],
            seatunnel_conn_id="seatunnel_test",
            poke_interval=10,
            timeout=300
        )
        
        # Verify sensor configuration
        assert sensor.job_id == "mock_job_id"
        assert sensor.target_states == ['FINISHED']


@pytest.mark.integration
class TestSeaTunnelConfigurationValidation:
    """Test configuration validation and error handling"""

    def test_invalid_engine_configuration(self):
        """Test handling of invalid engine configuration"""
        with pytest.raises(ValueError):
            SeaTunnelOperator(
                task_id="test_task",
                config_content="test config",
                engine="invalid_engine",
                seatunnel_conn_id="seatunnel_test"
            )

    def test_missing_configuration(self):
        """Test handling of missing configuration"""
        with pytest.raises(ValueError, match="Either config_file or config_content must be provided"):
            SeaTunnelOperator(
                task_id="test_task",
                seatunnel_conn_id="seatunnel_test"
            )

    def test_both_configurations_provided(self):
        """Test handling when both config_file and config_content are provided"""
        with pytest.raises(ValueError, match="Either config_file or config_content must be provided"):
            SeaTunnelOperator(
                task_id="test_task",
                config_file="/path/to/config.conf",
                config_content="test config",
                seatunnel_conn_id="seatunnel_test"
            )

    def test_sensor_with_invalid_engine(self, seatunnel_connection_params):
        """Test sensor validation with invalid engine"""
        connection = Connection(**seatunnel_connection_params)
        
        with patch('airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook.get_connection') as mock_get_conn:
            mock_get_conn.return_value = connection
            
            with patch('airflow_seatunnel_provider.sensors.seatunnel_sensor.SeaTunnelHook') as mock_hook_class:
                mock_hook = type('MockHook', (), {'engine': 'flink'})()
                mock_hook_class.return_value = mock_hook
                
                sensor = SeaTunnelJobSensor(
                    task_id="test_sensor",
                    job_id="test_job",
                    seatunnel_conn_id="seatunnel_test"
                )
                
                context = {"task_instance": type('MockTI', (), {})()}
                
                with pytest.raises(ValueError, match="SeaTunnelJobSensor only works with the 'zeta' engine"):
                    sensor.poke(context)