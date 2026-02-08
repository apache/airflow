from airflow.providers.common.ai.configs.datasource import DataSourceConfig
from airflow.sdk import BaseOperator


class LLMSQLQueryOperator(BaseOperator):
    """
    Operator to generate SQL query based on prompts
    """

    def __init__(self,
                 datasource: DataSourceConfig,
                 model_name: str | None = None,
                 pydantic_ai_conn_id: str = "pydantic_ai_default",
                 **kwargs):
        self.datasource = datasource
        self.model_name = model_name
        self.pydantic_ai_conn_id = pydantic_ai_conn_id
        super().__init__(**kwargs)

    def execute(self, context):
       self.log.info("Executing SQL query")
