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
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, teardown
from airflow.providers.openai.operators.openai import OpenAIEmbeddingOperator
from airflow.providers.pinecone.operators.pinecone import (
    CreatePodIndexOperator,
    PineconeIngestOperator,
)

index_name = os.getenv("INDEX_NAME", "example-pinecone-index")
namespace = os.getenv("NAMESPACE", "example-pinecone-index")


data = [
    """Title: The Evolution of Artificial Intelligence: Past, Present, and Future

Introduction

Artificial Intelligence (AI) has been one of the most transformative and rapidly advancing fields in modern technology. It has not only changed the way we interact with machines but also revolutionized various industries, from healthcare and finance to transportation and entertainment. This essay delves into the evolution of AI, covering its origins, current state, and the exciting prospects it holds for the future.

I. The Birth of Artificial Intelligence

AI's journey began in the mid-20th century when computer scientists and mathematicians sought to replicate human intelligence using machines. The founding fathers of AI, John McCarthy, Marvin Minsky, Allen Newell, and Herbert Simon, laid the theoretical groundwork for AI at the Dartmouth Workshop in 1956. They aimed to create machines that could mimic human reasoning and problem-solving.

Early AI research primarily focused on symbolic AI, where knowledge was represented using symbols, rules, and logic. This approach led to early successes in rule-based expert systems, such as Dendral, which could analyze chemical mass spectrometry data and diagnose diseases like expert human chemists.

II. The AI Winter and the Rise of Machine Learning

Despite promising beginnings, AI experienced a series of "AI winters" in the 1970s and 1980s. Funding dried up, and the field stagnated due to unrealistic expectations and the limitations of symbolic AI. However, AI made a resurgence in the late 20th century with the advent of machine learning and neural networks.

Machine learning, a subset of AI, gave machines the ability to learn from data, adapting and improving their performance over time. Neural networks, inspired by the structure of the human brain, became instrumental in pattern recognition, speech recognition, and image analysis. Breakthroughs like backpropagation and deep learning methods led to impressive advancements in areas like natural language processing and computer vision.

III. The Current State of Artificial Intelligence

Today, AI is part of our daily lives in ways we might not even notice. Virtual personal assistants like Siri and Alexa, recommendation systems on Netflix, and the algorithms behind search engines like Google are all powered by AI. Moreover, AI has found applications in healthcare, where it aids in diagnosis and drug discovery, and in autonomous vehicles, transforming the future of transportation.

One of the most exciting developments in AI is the use of Generative Adversarial Networks (GANs), which can generate realistic images, videos, and even text. This technology has opened up new possibilities in creative fields, from art and music to storytelling.

IV. Ethical Concerns and Challenges

As AI continues to evolve, it raises significant ethical concerns and challenges. Issues related to bias and fairness in AI algorithms have garnered attention, as they can perpetuate societal inequalities. Privacy concerns are another critical area, with AI systems capable of mining vast amounts of personal data. The potential for AI to displace human jobs and its implications for the job market are also subjects of ongoing debate.

V. The Future of AI

The future of AI is both promising and uncertain. We can expect to see AI playing a more significant role in decision-making processes across various industries, including finance, healthcare, and education. AI-powered chatbots and virtual assistants will become more sophisticated, enhancing user experiences.

However, AI's future also holds challenges. Ensuring that AI is developed and used responsibly, with transparency and fairness, is a priority. Continued research into AI safety and robustness will be essential, especially as we consider the deployment of AI in critical systems like autonomous vehicles and healthcare.

Conclusion

Artificial Intelligence has come a long way from its humble beginnings at the Dartmouth Workshop in 1956. The field has seen dramatic shifts in its approaches and applications, from symbolic AI to machine learning and deep learning. Today, AI is embedded in our lives, shaping the way we work, interact, and live. While the future of AI holds great promise, it also demands careful consideration of ethical and societal implications. With responsible development and continued innovation, AI will continue to be a driving force in shaping the future of humanity."""
]

with DAG(
    "example_pinecone_openai",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    create_index = CreatePodIndexOperator(
        task_id="create_index",
        index_name=index_name,
        dimension=1536,
    )

    embed_task = OpenAIEmbeddingOperator(
        task_id="embed_task",
        conn_id="openai_default",
        input_text=data,
        model="text-embedding-ada-002",
    )

    perform_ingestion = PineconeIngestOperator(
        task_id="perform_ingestion",
        index_name=index_name,
        input_vectors=[
            ("id1", embed_task.output),
        ],
        namespace=namespace,
        batch_size=1,
    )

    @teardown
    @task
    def delete_index():
        from airflow.providers.pinecone.hooks.pinecone import PineconeHook

        hook = PineconeHook()
        hook.delete_index(index_name=index_name)

    create_index >> embed_task >> perform_ingestion >> delete_index()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
