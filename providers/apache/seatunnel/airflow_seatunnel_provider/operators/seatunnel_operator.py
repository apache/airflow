from typing import Any, Dict, Optional, Union

from airflow.models import BaseOperator
from airflow_seatunnel_provider.hooks.seatunnel_hook import SeaTunnelHook
import os


class SeaTunnelOperator(BaseOperator):
    """
    Execute a SeaTunnel job.
    
    :param config_file: Path to the SeaTunnel configuration file, mutually exclusive with config_content.
    :param config_content: Content of the SeaTunnel configuration, mutually exclusive with config_file.
    :param engine: Engine to use (flink, spark, or zeta). Default is 'zeta'.
    :param seatunnel_conn_id: Connection ID to use.
    """
    
    template_fields = ('config_file', 'config_content', 'engine')
    template_ext = ('.conf', '.hocon')
    ui_color = '#1CB8FF'  # SeaTunnel blue color
    
    def __init__(
        self,
        *,
        config_file: Optional[str] = None,
        config_content: Optional[str] = None,
        engine: str = "zeta",
        seatunnel_conn_id: str = "seatunnel_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        
        if (config_file is None and config_content is None) or (config_file is not None and config_content is not None):
            raise ValueError("Either config_file or config_content must be provided, but not both.")
            
        self.config_file = config_file
        self.config_content = config_content
        self.engine = engine
        self.seatunnel_conn_id = seatunnel_conn_id
        self.temp_config_file = None
        
    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the SeaTunnel job.
        
        :param context: Airflow context.
        """
        hook = SeaTunnelHook(seatunnel_conn_id=self.seatunnel_conn_id, engine=self.engine)
        
        config_file_to_use = self.config_file
        
        # If config_content is provided, create a temporary file
        if self.config_content:
            self.temp_config_file = hook.create_temp_config(self.config_content)
            config_file_to_use = self.temp_config_file
        
        try:
            output = hook.run_job(config_file=config_file_to_use, engine=self.engine)
            self.log.info(f"SeaTunnel job completed successfully")
            return output
        finally:
            # Clean up temporary file if it was created
            if self.temp_config_file and os.path.exists(self.temp_config_file):
                os.unlink(self.temp_config_file)
                self.log.info(f"Deleted temporary config file: {self.temp_config_file}")