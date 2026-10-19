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
Inside a list of types (``[MyModel, int]``) the model is pushed as a ``dict`` instead.

The declared ``output_type`` (and any ``BaseModel`` reachable from
``Union``/``Optional``/``list[...]`` types) is registered for XCom deserialization by
the worker when it loads the Dag, before any task runs -- so no edit to
``[core] allowed_deserialization_classes`` is needed. The Pydantic class must be
defined at **module scope** and bound to an attribute matching its ``__name__``;
classes nested inside a function or ``@dag``-decorated body, parameterized
generics, and dynamically-built classes whose ``__name__`` does not match the
attribute they are bound to cannot be re-imported, so they are skipped with a
warning at worker startup and the value fails to deserialize at the consumer.
A list of types (``output_type=[MyModel, int]``) registers no class, so the operator pushes the
model as a ``dict`` even when ``serialize_output`` is ``False``.

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

The worker finds no class to register when ``output_type`` is a list of output
types such as ``[Summary, int]``, an output marker such as
``ToolOutput(Summary)``, an output function, a ``TypeAliasType`` or a
``NewType``. For these output types, ``AgentOperator`` and ``@task.agent`` push
each model in the output as a JSON-compatible dict, including models inside a
list, tuple or dict. A model is pushed as a dict even if its class is listed in
``[core] allowed_deserialization_classes``, or if another task in the same Dag
declares the class as its ``output_type``. A downstream task therefore reads
``result["field"]``, not ``result.field``.

In a JSON-compatible dict, values such as ``datetime``, ``date``, ``UUID`` and
``Decimal`` arrive as strings, tuples and sets arrive as lists, and ``Enum``
members arrive as their values.

A model at the top level is still dumped with plain ``model_dump()`` when
``serialize_output=True`` is set or on Airflow versions before 3.3. Either
condition is enough. A plain ``Enum`` member (one that does not also subclass
``str`` or ``int``), an ``HttpUrl`` or a ``Path`` in that model then stays a
Python object. The default XCom backend cannot serialize these objects, so the
XCom push fails with ``TypeError``. Before Airflow 3.3 the worker registers no
model class. ``AgentOperator`` and ``@task.agent`` therefore push a model
inside a list, tuple or dict as a JSON-compatible dict for every
``output_type``, not only for the output types listed above.

Upgrading from common.ai 1.0.0 or 1.1.0 can change the type that a downstream
task receives:

* On Airflow 3.3 and later, common.ai 1.0.0 and 1.1.0 pushed each model in the
  output as a model instance when ``output_type`` was one of the output types
  listed above. They dumped a top-level model with plain ``model_dump()`` only
  when ``serialize_output=True`` was set or in HITL review. A downstream task
  loaded the instance when the class was listed in
  ``[core] allowed_deserialization_classes`` or when another task in the same
  Dag declared the class as its ``output_type``. That downstream task now gets
  a dict, and ``result.field`` raises ``AttributeError``.
* In HITL review on Airflow 3.3 and later without ``serialize_output=True``,
  common.ai 1.0.0 and 1.1.0 dumped a top-level model of these output types with
  plain ``model_dump()`` after the reviewer approved the model. Values such as
  ``datetime``, ``date``, ``timedelta``, ``UUID`` and ``Decimal`` in that model
  now arrive as strings, for example ``"PT1M30S"`` for a ``timedelta``. Tuples
  and sets in that model now arrive as lists.
* Before Airflow 3.3, common.ai 1.0.0 and 1.1.0 pushed a model inside a list,
  tuple or dict as a model instance, for every ``output_type``. A downstream
  task that loaded the instance because the class was listed in
  ``[core] allowed_deserialization_classes`` now gets a dict.

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
operator calls ``model_dump()`` before pushing to XCom.
