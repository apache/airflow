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
runs like any other source. The tasks can be ordinary Airflow operators,
or a pointer to a task handler implemented in a Language SDK. (Support for the
Python Task SDK is planned for a future release.)

A document is declarative. Everything in it is a value, and the shape of each
value decides what it means.

The following is an example YAML file defining a Dag:

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

This Dag runs every day at 03:00. The first task ``extract`` renders the date
into its command. The second ``publish`` waits for ``extract`` through
``needs`` and runs a fixed command.

Enabling the importer
---------------------

The YAML importer is opt-in. Register it in
:ref:`config:dag_processor__dag_importer_configs`:

.. code-block:: ini

    [dag_processor]
    dag_importer_configs = [{"classpath": "airflow.sdk.importers.yaml_importer.YamlDagImporter"}]

Set this same configuration on every component that loads Dags, including the
Dag processor, the workers, and wherever ``airflow dags`` commands run.

The importer handles ``.yaml`` and ``.yml`` files that declare a Dag ``$schema``
(see below).

A document can also be written as JSON, which the YAML importer reads as-is.
Discovery is still by file extension, so write such a document in a ``.yaml`` or
``.yml`` file, or add ``.json`` to the importer's ``extensions``:

.. code-block:: ini

    [dag_processor]
    dag_importer_configs = [
        {
          "classpath": "airflow.sdk.importers.yaml_importer.YamlDagImporter",
          "extensions": [".yaml", ".yml", ".json"]
        }
      ]

For per-bundle registration and other options, see
:doc:`../administration-and-deployment/dag-importers`.

The shape of a document
-----------------------

``$schema`` (required)
    The format version the document is written against, given as the URL of a
    published schema. The version it names selects the rules the document is
    parsed with. ``$schema`` is also how Airflow recognizes that a file contains
    Dags. By default (see :ref:`config:core__dag_discovery_safe_mode`), a file
    that does not carry the canonical ``$schema`` URL is skipped, so a mistyped
    URL makes the file disappear the same way. An editor that understands
    ``$schema`` can offer validation and completion from it.

Dag-level keys
    Dag attributes sit at the top level, such as ``dag_id``, ``schedule``,
    ``params``. An unknown key is reported as an error.

``tasks``
    A list of tasks. Each task has an ``id`` and at most one body.

Task body: ``uses`` or ``run``
    ``uses:`` names a ready-made operator by its fully-qualified Python import
    path (e.g. ``airflow.providers.standard.operators.bash.BashOperator``).
    ``run:`` holds the arguments of a code task you implement elsewhere (see
    `Code tasks`_ below). A task can have one or the other. It can even have
    neither; such task becomes an ``EmptyOperator`` by default, which is handy
    as a join or marker point.

``with``
    Operator-specific arguments. These are what you would pass to the operator's
    constructor. Read on to understand the value grammar you can use in these
    arguments.

Task-level keys
    Scheduling and common attributes sit next to the body, distinct from
    ``with:``. These include things like ``retries``, ``trigger_rule``,
    ``queue``, etc. Any key that is not a structural key (``id``, ``uses``,
    ``run``, ``with``, ``needs``, ``extends``) must be a base operator argument;
    an operator-specific argument goes under ``with:`` instead, and a key that
    is neither is an error.

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

A value under ``with:`` (or ``run:``, with the restrictions in `Code tasks`_) is
taken literally, unless it is a single-key object whose key is a reserved
``$``-marker. For example, ``"echo foo.sh"`` and ``"{{ not rendered }}"`` are
taken as exactly that string, not a template, even if ``bash_command`` is listed
on the operator as a template field.

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
    The contents of a file are read and used. The path is resolved against the
    directory containing the current YAML document. A path that leaves the
    containing directory after resolution (including symlinks) is rejected.
    ``{$t: {$f: path}}`` allows reading the file and then rendering its contents
    as a template. The long form is ``{$file: path}``. A file is loaded only
    when you ask with ``$f``; it is never inferred from a filename suffix.

``{$const: value}`` is a forced literal
    This can be used to wrap contents that would otherwise carry a reserved ``$``
    meaning. For example, ``{$const: {$x: 1}}`` is the dict ``{"$x": 1}``, not
    an XCom.

The ``$t``, ``$x`` and ``$f`` markers may only be used on a field specified by
the operator as template-able (listed in ``template_fields``). Using them on any
other field causes an error. ``$const`` and plain values may be used anywhere.
For an operator you write yourself, the operator decides which fields may accept
markers; see `Using your own operators`_.

The ``$`` prefix in mapping keys is reserved for these markers. Prefixing any
other key with ``$`` causes a parse error. To include a key that must start with
``$``, wrap the mapping in ``$const``.

Markers nest. A marker inside a list, or inside a nested mapping such as an
``env`` dict, is resolved by the same rules, so references and templates compose
through structure:

.. code-block:: yaml

    with:
      env:
        RUN_DATE:
          $t: "{{ ds }}" # rendered
        REGION: us-east-1 # literal

Dates, datetimes, and deltas
----------------------------

A :class:`~datetime.date` or :class:`~datetime.datetime` argument accepts an
ISO-8601 string or a YAML-native value, and they mean the same thing:

.. code-block:: yaml

    start_date: "2024-01-01" # ISO-8601 string
    # start_date: 2024-01-01 # or a YAML-native date (same value)
    end_date: "2024-06-01T12:00:00" # ISO-8601 datetime string
    # end_date: 2024-06-01 12:00:00 # or a YAML-native datetime (same value)

A bare date on a :class:`~datetime.datetime` argument becomes midnight. An
ISO-8601 string is parsed.

A :class:`~datetime.timedelta` or :class:`~dateutil.relativedelta.relativedelta`
argument accepts a duration string, written in a short form or as an ISO-8601
duration:

.. code-block:: yaml

    retry_delay: 5m # short form, 5 minutes
    execution_timeout: PT1H # ISO-8601 duration, 1 hour

The short form combines ``w``, ``d``, ``h``, ``m`` and ``s`` (for example
``1h30m``) and builds a ``timedelta``. The `ISO-8601 duration
<https://en.wikipedia.org/wiki/ISO_8601#Durations>`_ form (``P...``) can build a
``timedelta`` or ``relativedelta``. Only whole-number components are
accepted, so a fractional value like ``PT0.5H`` is not recognized; write
``PT30M`` instead.

Each conversion applies only to an argument whose **type annotation** names the
matching type; a value bound to any other argument keeps its original type.

Using your own operators
------------------------

``uses:`` takes any operator's fully-qualified Python import path, so your own
operator class can work exactly like a provider's.

.. code-block:: yaml

    - id: ingest
      uses: my_company.operators.IngestOperator
      trigger_rule: all_done # base arguments at the task level
      with: # your operator's own arguments
        source: s3://bucket/incoming
        cutoff: 2024-01-01

An operator takes two kinds of argument, kept in separate places. Your
operator's own constructor arguments go under ``with:``. The arguments every
operator inherits from the base operator, such as ``retries``, ``retry_delay``,
``trigger_rule``, ``queue``, ``pool``, ``priority_weight``, ``depends_on_past``,
``start_date``, and the rest of the task model go at the task level beside the
body. Placing a base argument under ``with:``, or an operator-specific one at
the task level, is an error, so each argument has a single home.

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

A task argument is literal by default; only a value marked with ``{$t: ...}``
is rendered as a Jinja template. As `Values are literal by default`_ notes, a
marker may be used only on a field the operator templates, so an operator you
write must list the field in its ``template_fields`` before a marker works on it.

A field left unset keeps the operator's own default, including any templating
the operator applies to that default. An operator can also derive one of its own
templated fields from the arguments you give it, and that derived field still
renders, so an unmarked value can reach a rendered field indirectly. A value is
also never read from a file because of its name or suffix (in other words,
Python's ``template_ext`` does not apply when the operator is used in YAML); a
file is loaded only where you ask with ``{$f: ...}``.

Literal values inside a rendered container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A literal also stays literal when it sits in the same list or mapping as a
``{$t: ...}`` or ``{$x: ...}``. The marked entry renders, and the plain entries
beside it do not. Most operators receive the finished value and never notice. An
operator whose ``__init__`` reads the individual entries of such a container may
notice edge cases, but operator authors should be aware that argument processing
in ``__init__`` is generally considered bad practice in the first place. Such
processing should generally be done in ``execute`` instead, which is always
called after template rendering has been performed.

Accepting a date, datetime, or delta argument
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whether a value is converted depends on how the receiving argument is annotated.
An argument annotated as a date, datetime, or delta type receives the conversion
described in `Dates, datetimes, and deltas`_; every other argument is left as
written. To let your operator take these values, annotate the arguments:

.. code-block:: python

    import datetime

    from airflow.sdk import BaseOperator


    class IngestOperator(BaseOperator):
        def __init__(
            self,
            *,
            cutoff: datetime.datetime | None = None,
            retention: datetime.timedelta | None = None,
            **kwargs,
        ):
            super().__init__(**kwargs)
            self.cutoff = cutoff
            self.retention = retention

With the annotations above, ``cutoff: 2024-01-01`` arrives as
``datetime.datetime(2024, 1, 1, 0, 0)`` and ``retention: 5d`` as
``datetime.timedelta(days=5)``. Pendulum types are also coerced automatically.

The annotation is matched by the name, so the following are all recognized
(plus other similar variants):

* ``datetime.datetime``
* ``pendulum.DateTime``
* ``datetime.date``
* ``pendulum.Date``
* ``datetime.timedelta``
* ``dateutil.relativedelta.relativedelta``
* ``pendulum.Duration``

Unions are also parsed, so things like ``datetime | None`` are recognized too.
A type imported under another alias is not.

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
* an object ``{uses: <timetable import path>, with: {...}}`` to construct a
  timetable, the same ``uses`` / ``with`` idea used for operators.

Values under a timetable's ``with:`` receive the same date and delta coercion as
operator arguments (see `Dates, datetimes, and deltas`_), matched against the
timetable's constructor annotations. Value markers (``$t``, ``$x``, ``$f``) are
not interpreted there since timetables do not accept execution-time resolution.

Support for asset-triggering is planned for a future release.

Code tasks
----------

A task can run your own code instead of a ready-made operator with ``run:``,
which holds the code's arguments directly:

.. code-block:: yaml

    - id: transform
      queue: go-workers # to route to a coordinator
      run:
        source: {$x: extract}
        factor: 2

A ``run:`` task is executed by a Task SDK runtime, so it must be routed to a
coordinator. In practice, a ``run:`` task always needs a task-level ``queue:``
mapped to a coordinator under ``[sdk] queue_to_coordinator``. See
:doc:`language-sdks/index` for writing the handler and configuring the
coordinator.

Each argument in ``run:`` is either a JSON-serializable literal, or an upstream
task's output, in the ``{$x: task}`` form, which also wires the dependency.
``$f`` and ``$const`` resolve to literals. Due to restrictions imposed by the
cross-language task handler design, the ``run:`` syntax does not support
a template (``$t``), a ``$x`` referencing a custom XCom key (not the return
value), or an ``$x`` nested inside a larger value.

Current limitations
-------------------

Some features are not yet available when a Dag is defined in YAML. These may be
implemented in a future release.

* **Assets.** A task's ``inlets`` and ``outlets`` are rejected, and a Dag cannot
  be scheduled on an asset (see `Scheduling`_).
* **Task groups.** Tasks cannot be nested into groups (see `Reserved
  arguments`_).
* **Dynamic task mapping.** There is no equivalent of ``.expand()``.
* **Arguments that must be a Python object.** This particularly affects
  callbacks (which need a Python function).

A rejected key is reported as an error rather than silently ignored, as is any
other argument the target operator does not accept.
