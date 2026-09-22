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

.. _structured-output:

Structured output and XCom
==========================

Set ``output_type`` to a Pydantic ``BaseModel`` subclass. The LLM is instructed
to return structured data, and the model instance is pushed to XCom unchanged
so downstream tasks can type-hint the class directly
(``def downstream(result: MyModel)``) and use attribute access (``result.field``).

The declared ``output_type`` (and any ``BaseModel`` reachable from
``Union``/``Optional``/``list`` shapes) is registered for XCom deserialization by
the worker when it loads the Dag, before any task runs -- so no edit to
``[core] allowed_deserialization_classes`` is needed. The Pydantic class must be
defined at **module scope** and bound to an attribute matching its ``__name__``;
classes nested inside a function or ``@dag``-decorated body, parameterized
generics, and dynamically-built classes whose ``__name__`` does not match the
attribute they are bound to cannot be re-imported, so they are skipped with a
warning at worker startup and the value fails to deserialize at the consumer.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_structured_output_class]
    :end-before: [END howto_operator_llm_structured_output_class]

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_structured]
    :end-before: [END howto_operator_llm_structured]

Registration covers downstream tasks in the **same Dag**: every worker walks the
loaded Dag's tasks at startup and registers each declared class, so it also works
for mapped producers (``.expand(...)``) and for workers that load Dags from a
cache that bypasses operator construction.

The Airflow UI's XCom viewer renders Pydantic instances via the
``stringify`` path, which produces a representation like
``my_module.MyModel@version=1(field=value,...)`` without consulting the
allow-list. It is not pretty (no field-by-field rendering today), but the value
shows up; no configuration is required.

The remaining gap is **cross-Dag** ``xcom_pull`` -- a task in a different Dag
that pulls this XCom only parses its own Dag file, not the producer's, so the
class is not auto-registered. Add the class qualified name to
``[core] allowed_deserialization_classes`` (or a glob that matches it) to make
that pattern work.

If a downstream consumer needs the dict shape (e.g. forwarding to an external
system that expects JSON-style payloads), pass ``serialize_output=True`` and the
operator calls ``model_dump()`` before pushing to XCom. The pre-PR behavior is
available on demand without giving up the typed default.
