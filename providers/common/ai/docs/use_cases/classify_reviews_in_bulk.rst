 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Classify reviews in bulk at half the price
==========================================

A day of product reviews needs sentiment labels, nobody needs them in the next minute, and
a hundred thousand full-price calls is hard to justify. This Dag sends them all to the
vendor's batch API in one job at about half the per-request price, with up to 24 hours'
turnaround. Airflow defers while the batch runs so no worker is held, re-attaches to the
same batch on retry instead of paying twice, and lands results as JSONL on object storage
with only a manifest in XCom.

What this demonstrates
----------------------

* :doc:`../operators/llm_batch` -- ``LLMBatchOperator`` with ``deferrable=True`` and a
  ``result_path`` keyed by ``run_id``.
* :doc:`../structured_output` -- every request in the batch asks for the same
  ``Sentiment`` schema, and rows that fail validation are kept with their raw text.
* ``ObjectStoragePath`` -- the downstream task reads the landed rows back from storage
  rather than through XCom.

Run it
------

1. Install the provider with the OpenAI extra. Change ``model_id`` to run the same batch
   on Anthropic:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai]"

2. Change ``RESULT_ROOT`` in the file to a bucket you can write to, with an object storage
   connection for it.

3. Trigger the Dag. It defers until the vendor finishes the batch, which can take hours:

   .. code-block:: bash

       airflow dags test example_llm_batch_operator

The ``classify_reviews`` XCom is a manifest with the result URI and counts by status.
``summarize_manifest`` reads the JSONL and logs a count per label.

The Dag
-------

The output schema and the reader task, then the Dag body:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_structured_output_class]
    :end-before: [END howto_operator_llm_batch_structured_output_class]

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_read_results]
    :end-before: [END howto_operator_llm_batch_read_results]

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_basic]
    :end-before: [END howto_operator_llm_batch_basic]

Adapting it
-----------

* Replace ``REVIEWS`` with a task that pulls the day's reviews from your store and pass
  its output as ``requests``. The operator accepts a list of prompts or of
  per-request overrides; see :doc:`../operators/llm_batch`.
* Keep ``run_id`` in ``result_path``. A timestamp would break re-attachment on retry and
  let a flaky worker submit the batch twice; :doc:`../operators/llm_batch` explains why.
* Load the JSONL into your warehouse from a downstream task instead of counting labels,
  and emit an Asset so reporting Dags can run when the day's labels land.
