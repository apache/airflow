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
"""Example DAGs demonstrating LangChainHook usage patterns.

Each DAG covers a single pattern: chat-only, embedding-only, dual chat +
embedding, and separate connections for chat and embeddings. For a richer
end-to-end demo (ReAct agent, HITL review, vector retrieval), see
``example_langchain_tool_agent.py``.
"""

from __future__ import annotations

from airflow.providers.common.ai.hooks.langchain import LangChainHook
from airflow.providers.common.compat.sdk import dag, task


# [START howto_hook_langchain_chat]
@dag(schedule=None, tags=["example"])
def example_langchain_chat():
    @task
    def summarize(text: str) -> str:
        hook = LangChainHook(
            llm_conn_id="langchain_default",
            llm_model="openai:gpt-4o",
        )
        llm = hook.get_chat_model()
        # LangChain BaseMessage.content is `str | list[...]` (multi-modal union);
        # coerce to str for the text-only path this example demonstrates.
        return str(llm.invoke(f"Summarize concisely: {text}").content)

    summarize("Apache Airflow is a platform for authoring, scheduling, and monitoring workflows.")


# [END howto_hook_langchain_chat]

example_langchain_chat()


# [START howto_hook_langchain_embedding]
@dag(schedule=None, tags=["example"])
def example_langchain_embedding():
    @task
    def embed_documents(texts: list[str]) -> int:
        hook = LangChainHook(
            llm_conn_id="langchain_default",
            embed_model="openai:text-embedding-3-small",
        )
        embeddings = hook.get_embedding_model()
        vectors = embeddings.embed_documents(texts)
        return len(vectors[0])

    embed_documents(
        [
            "Apache Airflow is a workflow orchestrator.",
            "Workflows are defined as Python DAGs.",
        ]
    )


# [END howto_hook_langchain_embedding]

example_langchain_embedding()


# [START howto_hook_langchain_chat_and_embedding]
@dag(schedule=None, tags=["example"])
def example_langchain_chat_and_embedding():
    """One hook instance serves both chat and embeddings when both models are set."""

    @task
    def use_both() -> dict:
        hook = LangChainHook(
            llm_conn_id="langchain_default",
            llm_model="openai:gpt-4o",
            embed_model="openai:text-embedding-3-small",
        )
        chat = hook.get_chat_model()
        embeddings = hook.get_embedding_model()
        return {
            "answer": str(chat.invoke("In one sentence: what does Airflow do?").content),
            "embedding_dim": len(embeddings.embed_query("Airflow")),
        }

    use_both()


# [END howto_hook_langchain_chat_and_embedding]

example_langchain_chat_and_embedding()


# [START howto_hook_langchain_different_conns]
@dag(schedule=None, tags=["example"])
def example_langchain_different_conns():
    """Use separate connections when chat and embeddings live on different API keys."""

    @task
    def use_separate_conns() -> dict:
        hook = LangChainHook(
            llm_conn_id="openai_chat",
            embed_conn_id="openai_embed",
            llm_model="openai:gpt-4o",
            embed_model="openai:text-embedding-3-small",
        )
        chat = hook.get_chat_model()
        embeddings = hook.get_embedding_model()
        return {
            "answer": str(chat.invoke("In one sentence: what does Airflow do?").content),
            "embedding_dim": len(embeddings.embed_query("Airflow")),
        }

    use_separate_conns()


# [END howto_hook_langchain_different_conns]

example_langchain_different_conns()
