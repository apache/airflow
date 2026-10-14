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

.. _yaml-dags:

Writing Dags in YAML
====================

|experimental|

.. versionadded:: 3.4.0

Airflow can build a Dag from a YAML document. One YAML document describes one
Dag, and the YAML Dag importer turns it into the same Dag structure that Airflow
runs like any other source. The tasks can be ordinary Airflow operators, so
every provider operator is available without writing code.

A document is declarative. Everything in it is a value, and the shape of each
value decides what it means. Since the format round-trips through JSON, a
document may equally be written as JSON.

The following is a example YAML file defining a Dag:

.. code-block:: yaml

    $schema: https://airflow.apache.org/schemas/dag/2026-10-30.json
    dag_id: daily_report
    schedule: "0 3 * * *"
    start_date: 2024-01-01

    tasks:
      - id: extract
        uses: airflow.providers.standard.operators.bash.BashOperator
        with:
          bash_command:
            $t: "extract.sh {{ ds }}"

      - id: publish
        uses: airflow.providers.standard.operators.bash.BashOperator
        needs: [extract]
        with:
          bash_command: "publish.sh"

This Dag runs every day at 03:00, The first task ``extract`` renders the date
into its command. The second ``publish`` waits for ``extract`` through
``needs`` and runs a fixed command.

Enabling the importer
---------------------

The YAML importer is opt-in. Register it in
:ref:`config:dag_processor__dag_importer_configs`:

.. code-block:: ini

    [dag_processor]
    dag_importer_configs = [{"classpath": "airflow.sdk.importers.yaml_importer.YamlDagImporter"}]

The importer handles ``.yaml`` and ``.yml`` files that declare a Dag ``$schema``
(see below). For per-bundle registration and other options, see
:doc:`../administration-and-deployment/dag-importers`.

The shape of a document
-----------------------

``$schema`` (required)
    The format version the document is written against, given as the URL of a
    published schema. The version it names selects the rules the document is
    parsed with. ``$schema`` is also how Airflow recognizes the file
    as a Dag; a file without it is silently skipped. Editors that understand
    ``$schema`` will offer validation and completion with no extra setup.

Dag-level keys
    Attribute of the Dag sit at the top level, such as ``dag_id``, ``schedule``,
    ``params``, etc. An unknown key is reported as an error.

``tasks``
    A list of tasks. Each task has an ``id`` and exactly one body.

Task body: ``uses`` or ``run``
    ``uses:`` names a ready-made operator by its fully-qualified Python import
    path (e.g. ``airflow.providers.standard.operators.bash.BashOperator``).
    ``run:`` holds the arguments of a code task you implement elsewhere (see
    `Code tasks`_ below). A task may have one or the other.

``with``
    Operator-specific arguments. These are what you would pass to the operator's
    constructor. Read on to understand the value grammar you can use in these
    arguments.

Task-level keys
    Scheduling and common attributes sit next to the body, distinct from
    ``with:``. These include things like ``retries``, ``trigger_rule``,
    ``queue``, etc. Any key that is not one of the structural keys (``id``,
    ``uses``, ``run``, ``with``, ``needs``, ``extends``) is passed to the
    operator type, which validates it.

``needs``
    A list of upstream tasks (referenced by ``id``) that must run before this
    one. This describes an explicit dependency edge; XCom references in
    ``with:`` or ``run:`` are discovered automatically and do not need to be
    repeated here.


Templates and extends
---------------------

Tasks often share arguments. You can define a named partial task under the
top-level ``templates:`` key, and pull it into a task with ``extends``:

.. code-block:: yaml

    templates:
      retryable:
        retries: 2
      shell:
        extends: [retryable] # a template can extend other templates
        uses: airflow.providers.standard.operators.bash.BashOperator
        with:
          cwd: /opt/airflow/scripts

    tasks:
      - id: extract
        extends: [shell] # pull the template into this task
        with:
          bash_command: "extract.sh"
      - id: load
        extends: [shell]
        with:
          bash_command: "load.sh"

Both tasks run a ``BashOperator`` with ``retries: 2`` and the shared ``cwd``,
and add only their own ``bash_command``. A template may set anything a task can,
and may itself ``extends`` other templates.

The templates named in ``extends`` are applied left to right, and the task's own
inline fields are applied last. Merging is shallow: ``with:`` and ``run:`` are
combined one key at a time, while every other field is taken as a whole rather
than deep-merged.

The assembled task is validated against the real operator, so a field the
operator does not accept is reported as an error. It is also an error if a
non-existent template is referenced.


Values are literal by default
-----------------------------

A value under ``with:`` (or ``run:``) is taken literally, unless it is a
single-key object whose key is a reserved ``$``-marker. For example,
``"echo foo.sh"`` and ``"{{ not rendered }}"`` specifies exactly that string,
not a template, even if ``bash_command`` is listed on the operator as a template
field.

Four markers opt a value into special handling:

``{$t: "..."}`` is a template string
    A Jinja template, rendered when the task runs. This is the only way a value
    is templated: ``{$t: "{{ ds }}"}``, ``{$t: "{{ params.region }}"}``,
    ``{$t: "{{ conn.my_db.host }}"}``. The long form ``{$template: "..."}`` is
    also accepted.

``{$x: id}`` references an upstream output
    The XCom output of an upstream task. ``{$x: extract}`` references
    ``extract``'s return value, and ``{$x: {task: extract, key: rows}}`` a
    specific key. Binding a value this way also creates the dependency edge
    implicitly, so a task that consumes another's output does not need to also
    list it in ``needs:``. The long form is ``{$xcom: ...}``.

``{$f: path}`` reads a file's contents
    The contents of a file stored next to the document is read and used.
    ``{$t: {$f: path}}`` allows reading the file and then rendering its contents
    as a template. The long form is ``{$file: path}``. A file is loaded only
    when you ask with ``$f``; it is never inferred from a filename suffix.

``{$const: value}`` is a forced literal
    An escape for the rare case that real data is shaped exactly like a marker.
    ``{$const: {$x: 1}}`` is the literal dict ``{"$x": 1}``, not an XCom.

Markers nest. A marker inside a list, or inside a nested mapping such as an
``env`` dict, is resolved by the same rules, so references and templates compose
through structure:

.. code-block:: yaml

    with:
      env:
        RUN_DATE:
          $t: "{{ ds }}" # rendered
        REGION: us-east-1 # literal

Dates and datetimes
-------------------

A date or datetime argument accepts an ISO-8601 string or a YAML-native date,
and they mean the same thing:

.. code-block:: yaml

    start_date: "2024-01-01" # ISO-8601 string
    start_date: 2024-01-01 # YAML date
    end_date: "2024-06-01T12:00:00" # ISO-8601 datetime string

These are converted to a Python ``datetime`` before the Dag or task is built. A bare date becomes
midnight, and an ISO-8601 string is parsed.

The conversion is applied only to an argument that expects a datetime. This is
decided from the operator's or Dag's argument **type annotation**.

Using your own operators
------------------------

``uses:`` takes any operator's fully-qualified Python import path, so your own
operator class can work exactly like a provider's.

.. code-block:: yaml

    - id: ingest
      uses: my_company.operators.IngestOperator
      trigger_rule: all_done # base arguments at the task-level
      with: # your operator's own arguments
        source: s3://bucket/incoming
        cutoff: 2024-01-01

An operator takes two kinds of argument, kept in separate places. Your operator's own constructor
arguments go under ``with:``. The arguments every operator inherits from the base operator --
``retries``, ``retry_delay``, ``trigger_rule``, ``queue``, ``pool``, ``priority_weight``,
``depends_on_past``, ``start_date``, and the rest of the task model -- go at the task level, beside
the body. Placing a base argument under ``with:``, or an operator-specific one at the task level, is
an error, so each argument has a single home.

The task is constructed roughly like this:

.. code-block:: python

    IngestOperator(
        task_id="ingest",
        source="s3://bucket/incoming",
        cutoff=datetime.datetime(2024, 1, 1, 0, 0),
        trigger_rule="all_done",
    )

Templated argument rendering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A task argument is literal by default. For one to be rendered as a Jinja
template, two things must satisfy: the operator must allow that field to be
templated (for Python operators, by specifying it in ``template_fields``), and
the Dag author must use the ``{$t: ...}`` syntax. A value not marked explicitly
stays literal. Try to use the template syntax on a field not specified by the
operator results in an error.

A field leave unset keeps the operator's own default, including any templating
the operator applies to that default. A value is also never read from a file
because of its name or suffix (Python's ``template_ext`` does not apply); a file
is loaded only where you ask with ``{$f: ...}``.

Literal values inside a rendered container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A literal also stays literal when it sits in the same list or mapping as a
``{$t: ...}`` or ``{$x: ...}``. The marked entry renders, and the plain entries
beside it do not. Most operators receive the finished value and never notice. An
operator whose ``__init__`` reads the individual entries of such a container may
notice edge cases, but operator authors should be aware that argument processing
in ``__init__`` is generally considered bad practice in the first place. Such
processing should generally be done in ``execute`` instead, which is always
called after template rendering has be performed.

Accepting a date argument
~~~~~~~~~~~~~~~~~~~~~~~~~

Whether a value is converted to a ``datetime`` depends on how the receiving
argument is typed. An argument whose type annotation names a datetime type
receives the conversion described in `Dates and datetimes`_; every other
argument is left as written. To let your operator take a YAML date, annotate the
argument:

.. code-block:: python

    import datetime

    from airflow.sdk import BaseOperator


    class IngestOperator(BaseOperator):
        def __init__(self, *, cutoff: datetime.datetime | None = None, **kwargs):
            super().__init__(**kwargs)
            self.cutoff = cutoff

With the annotation above, ``cutoff: 2024-01-01`` from YAML arrives as
``datetime.datetime(2024, 1, 1, 0, 0)``. The annotation is matched by name, so
``datetime``, ``datetime.datetime``, ``pendulum.DateTime``, and a union such as
``datetime | None`` are all recognized; a datetime type imported under another
alias is not.

Reserved arguments
~~~~~~~~~~~~~~~~~~

A few names are reserved and cannot be set on a task. The task id comes from
``id``, not ``task_id``; arguments ``dag``, ``task_group``, and ``default_args``
are not accepted. Use ``templates:`` and ``extends:`` for shared defaults in
place of ``default_args``. (Support for task groups is planned for a future
release.)

Scheduling
----------

``schedule:`` accepts the forms Airflow already understands, chosen by shape:

* a cron string (``"0 3 * * *"``) or a preset (``"@daily"``);
* ``null`` or an omitted key, for a Dag that is only triggered manually or
  externally;
* an object ``{uses: <timetable import path>, with: {...}}`` to construct any
  timetable, the same ``uses`` / ``with`` idea used for operators.

Support for asset-triggering is planned for a future release.

Code tasks
----------

A task can run your own code instead of a ready-made operator with ``run:``,
which holds the code's arguments directly:

.. code-block:: yaml

    - id: transform
      run:
        source: {$x: extract}
        factor: 2

A ``run:`` task is built into a stub that a Task SDK runtime executes. See
:doc:`language-sdks/index` to understand how to write a task handler for runtime
execution. A top-level ``{$x: ...}`` argument binds an upstream task's output;
every other argument must be a JSON-serializable literal.
