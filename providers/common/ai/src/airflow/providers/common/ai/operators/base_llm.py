from functools import cached_property

from pydantic_ai import Agent

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.sdk import BaseOperator


class BaseLLMOperator(BaseOperator):
    """
    Base operator for LLM-based tasks.
    """

    def __init__(self,
                 model_name: str | None = None,
                 pydantic_ai_conn_id: str = "pydantic_ai_default",
                 **kwargs):
        super().__init__(**kwargs)
        self.model_name = model_name
        self.pydantic_ai_conn_id = pydantic_ai_conn_id
        self._agent: Agent | None = None

    @cached_property
    def _hook(self):
        return PydanticAIHook(
            pydantic_ai_conn_id=self.pydantic_ai_conn_id,
            model_name=self.model_name
        )

    def _create_llm_agent(self, output_type, instructions=None):
        """ Create Pydantic AI agent. """

        model = self._hook.get_model()
        if self._agent is not None:
            return self._agent

        self._agent = Agent(model=model, output_type=output_type, instructions=instructions)
        return self._agent

    def _run_with_agent(self):
        pass

    def _prepare_prompts(self):
        pass

    def _process_llm_response(self):
        pass

    def execute(self, context):
        pass

