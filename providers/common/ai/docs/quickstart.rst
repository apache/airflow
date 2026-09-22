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

.. _howto/quickstart:

Quick start
===========

In five steps you install the provider, connect it to a model vendor, and run a Dag in
which one task asks a model to summarize release notes and a second task uses the answer.
At the end you know where the model's output lands and what a successful run looks like.

You need a working :doc:`Airflow installation <apache-airflow:installation/index>` on
Airflow 3.0 or later and an API key for the model vendor you plan to use. Step 4 makes one
real, billed API call.

1. Install the provider
-----------------------

Install the extra that matches your model vendor. The example below uses OpenAI; swap the
extra for ``anthropic``, ``google`` or ``bedrock`` if that is what you have
(:doc:`model_providers` lists every vendor):

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[openai]"

2. Create the connection
------------------------

Model calls go through an Airflow connection of type ``pydanticai``. The connection holds
the API key and the model name in ``provider:model`` form, so switching vendors later is a
connection change, not a Dag change. The Dag below uses the default connection id,
``pydanticai_default``.

The quickest way to create it is an environment variable on the machine that runs the
scheduler and the workers. Replace ``sk-...`` with your key and ``openai:gpt-5.6-sol`` with
a model you have access to:

.. code-block:: bash

    export AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai", "password": "sk-...", "extra": {"model": "openai:gpt-5.6-sol"}}'

You can also create it in the UI under **Admin > Connections**: choose the connection type
**Pydantic AI**, set the connection id to ``pydanticai_default``, put the API key in
**Password** and the model in the **Model** field. Vendors that authenticate through the
environment instead of a key (Bedrock, Vertex AI) have their own connection types; see
:doc:`connections/pydantic_ai`.

3. Save the Dag
---------------

Save the following as ``quickstart_llm.py`` in your Dags folder, the directory
``[core] dags_folder`` points at (``$AIRFLOW_HOME/dags`` by default):

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_quickstart.py
    :language: python
    :start-after: [START howto_quickstart_llm]
    :end-before: [END howto_quickstart_llm]

Two tasks. ``summarize`` is a ``@task.llm`` task: the function returns the prompt, the
decorator sends it to the model on ``pydanticai_default`` and pushes the model's reply to
XCom as the task's return value. ``publish`` is an ordinary ``@task`` that receives that
reply as its argument, the same way any Airflow task receives an upstream result, logs it,
and returns a small dict of its own.

4. Run it
---------

Run the Dag once in the foreground from the machine where you saved the file:

.. code-block:: bash

    airflow dags test quickstart_llm

The command parses the Dag, runs ``summarize`` and then ``publish``, and prints both task
logs to the terminal. You can also trigger the Dag from the UI with the play button on the
``quickstart_llm`` row of the Dags list.

5. Check the result
-------------------

In the terminal output, or in each task's log in the UI, look for two lines:

- From ``summarize``, a line starting ``LLM run complete`` with the model name and the
  token counts. This is the provider's post-run summary, and it appears after every model
  call.
- From ``publish``, a line starting ``Release summary:`` followed by the two sentences the
  model wrote.

In the UI, open the run in the Grid view, select the ``summarize`` task and open its
**XCom** tab: the ``return_value`` entry holds the model's reply. The ``publish`` task's
``return_value`` is the dict with the summary and its length.

If the run fails before the model is called, the error names what is missing: a
connection without a model, a model name without a ``provider:`` prefix, or a vendor
package that is not installed. :doc:`troubleshooting` lists each message with its fix.

Where to go next
----------------

- :doc:`use_cases/index` shows jobs a data team already has, each with the Dag that does
  it.
- :doc:`structured_output` returns a typed Pydantic object instead of a string, so the
  downstream task gets fields rather than prose.
- :doc:`operators/index` picks the operator for a job: branching on an answer, analyzing
  files, generating SQL, batch processing.
- :doc:`operators/agent` gives the model tools built from Airflow hooks, SQL databases or
  MCP servers, so it can act instead of only answering.
