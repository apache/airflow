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

.. NOTE TO CONTRIBUTORS:
    Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
    and you want to add an explanation to the users on how they are supposed to deal with them.
    The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-common-ai``

Changelog
---------

Breaking change: operators with ``output_type=<BaseModel subclass>``
(``LLMOperator``, ``LLMAgentOperator``, ``LLMFileAnalysisOperator``, and
their ``@task.llm`` / ``@task.agent`` / ``@task.llm_file_analysis`` decorators)
now return the Pydantic model instance through XCom instead of dumping it to
a ``dict``, on Airflow versions whose worker registers operator-declared output
classes for deserialization. Downstream tasks should type-hint the model class
(``def downstream(result: MyModel)``) and use attribute access (``result.field``)
instead of subscript access. The output class must be defined at **module scope**
and bound to an attribute matching its ``__name__``; classes that are nested,
dynamically built, or otherwise non-importable by ``qualname`` cannot be
re-imported and will fail to deserialize at the consumer.

The worker walks the loaded DAG and registers each declared class before any
task runs, so same-DAG downstream tasks (including mapped ``.expand(...)``
producers) deserialize the model without any configuration change. The UI XCom
viewer renders the value via the ``stringify`` path and works without
configuration (it shows ``module.MyModel@version=1(field=value,...)`` rather than
a pretty form). Cross-DAG ``xcom_pull`` consumers still need the class qualified
name added to ``[core] allowed_deserialization_classes`` -- the consumer DAG's
worker only loads its own DAG. On Airflow versions whose worker does not register
declared classes, the operators dump to ``dict`` instead.

0.4.0
.....

.. note::
    This release changes the return type of ``LLMOperator``, ``LLMAgentOperator`` and
    ``LLMFileAnalysisOperator``: structured output is now returned through XCom as Pydantic
    model instances instead of plain ``dict`` objects. Downstream tasks that consume these
    XCom values must be updated accordingly. As this provider is still pre-1.0, the breaking
    change ships in a minor release.

Breaking changes
~~~~~~~~~~~~~~~~~~

* ``Return Pydantic model instances through XCom for structured output (#67644)``

Features
~~~~~~~~

* ``Add 'OpenTelemetry' tracing for 'common.ai' Pydantic AI agents (#67792)``
* ``Add a bridge to expose 'common.ai' toolsets as LangChain tools (#67791)``
* ``Add Agent Skills support to the Common AI provider (#67786)``
* ``Accept Sequence[UserContent] in common.ai TaskFlow decorators (#67389)``
* ``Add LlamaIndex operators to common.ai provider (#67121)``
* ``Add 'DocumentLoaderOperator' to 'common.ai' provider (#67120)``
* ``Add 'Langchain' hook to 'common-ai' provider (#67192)``

Bug Fixes
~~~~~~~~~

* ``Register operator-declared XCom classes from a worker-side DAG walk (#67875)``
* ``common-ai: Honour serialize_output=True on LLMFileAnalysisOperator (#67858)``

Misc
~~~~

* ``Bump common.ai floor to pydantic-ai-slim>=1.71.0 and document capabilities passthrough (#67444)``
* ``Remove further findings from positional session check (#67712)``
* ``Add prek hook to enforce HTTPException is imported from fastapi (#67367)``
* ``Add prek hook enforcing the "example" tag on example DAGs (#67354)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add SEC 10-K analysis example using LangChain for Common.ai provider (#67727)``
   * ``Add SEC 10-K financial analysis example DAG using LlamaIndex for Common.ai provider (#67671)``
   * ``Add AIP progress tracker example DAG for common.ai provider (#67660)``
   * ``[main] CI: Upgrade important CI environment (#67593)``
   * ``[main] CI: Upgrade important CI environment (#67313)``
   * ``Prevent durable storage tests from leaking hook lineage (#67252)``
   * ``Fix LangChain hook tests failing when langchain is not installed (#67237)``

0.3.0
.....

Features
~~~~~~~~

* ``Add 'LLMRetryPolicy' to common-ai provider (#65451)``

Bug Fixes
~~~~~~~~~

* ``Update dependencies to fix dependabot alarms in providers.common.ai (#66628)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] CI: Upgrade important CI environment (#66843)``

0.2.0
.....

Features
~~~~~~~~

* ``Add UsageLimits support to common.ai operators (#66248)``

Doc-only
~~~~~~~~

* ``Add Configuration Reference docs page to Common AI provider (#66024)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use contextlib.suppress instead of try-except-pass in providers (#66178)``
   * ``[main] CI: Upgrade important CI environment (#66068)``
   * ``fix: update dependencies to fix dependabot alarms in providers.common.ai (#66244)``
   * ``[main] CI: Upgrade important CI environment (#65933)``
   * ``Providers wave 2026-04-21 (#65614)``
   * ``Providers wave 2026-04-21``

0.1.1
.....

Misc
~~~~

* ``Update dependencies to address Dependabot security alarms in providers.common.ai (#65048)``
* ``Bump vite (#64799)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] CI: Upgrade important CI environment (#65521)``
   * ``Isolate non-provider mypy hooks per distribution with dedicated .build/ venvs (#65492)``
   * ``Simple LLM scenario example based on the Airflow survey data (#65172)``

0.1.0
.....

.. note::
  This release of provider is only available for Airflow 3.0+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.
