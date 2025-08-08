from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import subprocess
import os
import tempfile
import json


class SeaTunnelHook(BaseHook):
    """
    Hook to interact with Apache SeaTunnel.
    
    :param seatunnel_conn_id: Connection ID to fetch connection info.
    :param engine: Engine to use (flink, spark, or zeta). Default is 'zeta'.
    """

    conn_name_attr = "seatunnel_conn_id"
    default_conn_name = "seatunnel_default"
    conn_type = "seatunnel"
    hook_name = "Apache SeaTunnel"

    def __init__(
        self,
        seatunnel_conn_id: str = default_conn_name,
        engine: str = "zeta"
    ) -> None:
        super().__init__()
        self.seatunnel_conn_id = seatunnel_conn_id
        self.engine = engine
        self.conn = self.get_connection(seatunnel_conn_id)
        self.host = self.conn.host
        self.port = self.conn.port
        self.extra_dejson = self.conn.extra_dejson
        self.seatunnel_home = self.extra_dejson.get("seatunnel_home")
        if not self.seatunnel_home:
            raise AirflowException("SeaTunnel home directory must be specified in the connection.")

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behavior for connection form"""
        return {
            "hidden_fields": ["login", "password", "schema"],
            "relabeling": {
                "host": "SeaTunnel Host",
                "port": "SeaTunnel Port",
            },
            "placeholders": {
                "host": "localhost",
                "port": "8083",
                "extra": json.dumps(
                    {
                        "seatunnel_home": "/path/to/seatunnel",
                    },
                    indent=2,
                ),
            },
        }

    def run_job(self, config_file: str, engine: Optional[str] = None) -> str:
        """
        Run a SeaTunnel job with the given configuration file.
        
        :param config_file: Path to the SeaTunnel configuration file.
        :param engine: Engine to use (flink, spark, or zeta). Overrides the instance's engine.
        :return: The output of the command.
        """
        engine = engine or self.engine
        
        if engine not in ["flink", "spark", "zeta"]:
            raise AirflowException(f"Unsupported engine: {engine}. Must be one of 'flink', 'spark', or 'zeta'.")
        
        # Check config file first
        if not os.path.isfile(config_file):
            raise AirflowException(f"Config file not found at: {config_file}")
        
        # Determine the script to use based on the engine
        script_name = {
            "flink": "start-seatunnel-flink-connector-v2.sh",
            "spark": "start-seatunnel-spark-connector-v2.sh", 
            "zeta": "seatunnel.sh"
        }[engine]
        
        script_path = os.path.join(self.seatunnel_home, "bin", script_name)
        
        if not os.path.isfile(script_path):
            raise AirflowException(f"SeaTunnel script not found at: {script_path}")
        
        # Prepare the command
        if engine == "zeta":
            cmd = [script_path, "--config", config_file]
        else:
            cmd = [script_path, "--config", config_file, "--deploy", "local"]
        
        self.log.info(f"Executing command: {' '.join(cmd)}")
        
        # Execute the command and return the output
        try:
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            output = ""
            for line in process.stdout:
                output += line
                self.log.info(line.strip())
            
            process.wait()
            
            if process.returncode != 0:
                raise AirflowException(f"SeaTunnel job failed with return code {process.returncode}")
                
            return output
        except Exception as e:
            raise AirflowException(f"Failed to execute SeaTunnel job: {str(e)}")
    
    def create_temp_config(self, config_content: str) -> str:
        """
        Create a temporary file with the given configuration content.
        
        :param config_content: The content of the configuration file.
        :return: The path to the temporary file.
        """
        with tempfile.NamedTemporaryFile(suffix=".conf", delete=False) as temp_file:
            temp_file.write(config_content.encode("utf-8"))
            return temp_file.name 