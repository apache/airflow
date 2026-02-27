# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import logging
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Dict

from datafusion import SessionContext
from pydantic_ai import ModelRetry, RunContext, Agent

from airflow.providers.common.ai.config import StorageType
from airflow.providers.common.ai.datafusion.engine import DataFusionEngine
from airflow.providers.common.ai.operators.base_llm import BaseLLMOperator

log = logging.getLogger(__name__)

@dataclass
class DataAnalystAgentDeps:

    df_session_context: SessionContext
    # table_name, table_schem
    known_tables: Dict[str, str] = field(default_factory=dict)

    def refresh_tables(self):
        """Fetch tables and their schema using datafusion"""
        self.known_tables.clear()
        try:
            tables_df = self.df_session_context.sql("SHOW TABLES").to_pandas()
            for _, row in tables_df.iterrows():
                table_name = row['table_name']
                try:
                    schema = str(self.df_session_context.table(table_name).schema())
                    self.known_tables[table_name] = schema
                except Exception as e:
                    self.known_tables[table_name] = "(schema not available)"
        except Exception as e:
            print(f"Warning: could not refresh table list: {e}")

    def get_schema(self, table_name: str) -> str:
        log.debug("Getting schema for table: %s", table_name)
        if not self.known_tables:
            self.refresh_tables()

        if table_name not in self.known_tables:
            if table_name not in self.known_tables:
                raise ModelRetry(
                    f"Table '{table_name}' does not exist in the current DataFusion session.\n"
                    f"Known tables: {list(self.known_tables.keys()) or ['none']}"
                )
        return self.known_tables[table_name]

    def list_tables(self) -> str:
        log.debug("Listing tables")

        if not self.known_tables:
            self.refresh_tables()

        if not self.known_tables:
            return "No tables are currently registered in the DataFusion session."

        lines = ["Currently available tables:"]

        for name, schema in sorted(self.known_tables.items()):
            lines.append(f"• {name:<20} {schema[:140]}...")
        return "\n".join(lines)


class LLMFileAnalysisOperator(BaseLLMOperator):

    def __init__(self,
                 allowed_max_preview_rows: int = 5,
                 **kwargs):
        self.allowed_max_preview_rows = allowed_max_preview_rows
        super().__init__(**kwargs)


    @cached_property
    def _df_engine(self) -> DataFusionEngine:
        """Return the datafusion engine."""
        return DataFusionEngine()

    @property
    def _instruction(self):
        """Return the instruction for DataAnalystAgent"""

        instructions = """You are a SQL analyst working with pre-registered tables.

        Very important rules:

        1. ONLY use table names that really exist.
           → ALWAYS check the list of available tables first using list_tables tool.
           → NEVER guess or invent table names.

        2. Do NOT try to load files or register new tables — that has already been done outside this agent.

        3. Use describe_table to see the schema of any table before writing complex queries.

        4. All SQL must be valid Apache DataFusion SQL using only existing table names.

        5. run_datafusion shows a small preview automatically.
           Use display if you want more rows or different formatting.

        Start by calling list_tables to understand what data is available and use the describe_table to get schema of the table
        Make sure you finish all the prompts finished before providing the results
        """
        return instructions

    def get_prepared_prompt(self) -> str:
        """Prepare prompt input for LLM Agent"""
        prompts = "\n".join(f"{i+1}. {prompt}" for i, prompt in enumerate(self.prompts))
        prompts = prompts + "\n Finally consolidate all the above prompt results and return."
        self.log.info("Prepared prompt for LLM: \n\n%s", prompts)
        return prompts

    @staticmethod
    def list_tables(ctx: RunContext[DataAnalystAgentDeps]):
        """Return all tables that are registered in the current Apache DataFusion session."""
        return ctx.deps.list_tables()

    def execute(self, context) -> Any:
        try:
            for datasource_config in self.datasource_configs:
                if datasource_config.storage_type == StorageType.LOCAL:
                    connection_config = None
                else:
                    connection_config = self.get_conn_config_from_airflow_connection(
                        datasource_config.conn_id)

                self._df_engine.register_datasource(datasource_config, connection_config)

            agent = self._create_llm_agent(output_type=str, instruction=self._instruction, deps_type=DataAnalystAgentDeps)
            self.register_tools(agent)
        except Exception as e:
            log.error("Error creating agent: %s", e)
            raise

        dataanalyst_agent_dep = DataAnalystAgentDeps(df_session_context=self._df_engine.session_context)
        dataanalyst_agent_dep.refresh_tables()
        result = agent.run_sync(user_prompt=self.get_prepared_prompt(), deps=dataanalyst_agent_dep)

        log.info("Agent result: %s", result.output)

        return result.output

    def register_tools(self, agent: Agent):
        """Register all agent tools"""

        @agent.tool
        def list_tables(ctx: RunContext[DataAnalystAgentDeps]):
            """Return all tables that are registered in the current Apache DataFusion session."""
            return ctx.deps.list_tables()

        @agent.tool
        def describe_table(ctx: RunContext[DataAnalystAgentDeps], table_name: str) -> str:
            """Get the schema of a specific table."""
            try:
                schema = ctx.deps.get_schema(table_name)
                return f"Table: {table_name}\nSchema:\n{schema}"
            except ModelRetry as e:
                return str(e)

        @agent.tool
        def run_datafusion(ctx: RunContext[DataAnalystAgentDeps], sql: str) -> str:
            """Execute SQL against the current DataFusion session tables."""

            log.info("Running SQL: %s", sql)
            try:
                result = ctx.deps.df_session_context.sql(sql)
                preview = result.limit(self.allowed_max_preview_rows).to_pandas().to_string(index=False)
                return "\n".join([
                    "Query executed.",
                    "SQL:",
                    sql.strip(),
                    "",
                    f"Preview (up to {self.allowed_max_preview_rows} rows):",
                    preview,
                    "",
                    "Use display tool if you need more rows."
                ])
            except Exception as e:
                return f"Execution failed:\n{str(e)}\n\nPlease correct the SQL."

        @agent.tool
        def display(ctx: RunContext[DataAnalystAgentDeps], sql: str, rows: int = 10) -> str:
            """Execute SQL and show a preview of the requested number of rows."""
            try:
                result = ctx.deps.df_session_context.sql(sql)
                df = result.limit(rows).to_pandas()
                return f"Result preview ({rows} rows max):\n\n{df.to_string(index=False)}"
            except Exception as e:
                return f"Display failed: {str(e)}"
