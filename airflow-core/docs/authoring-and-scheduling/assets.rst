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

.. _asset_definitions:

Asset Definitions
=================

.. versionadded:: 2.4

.. versionchanged:: 3.0

  The concept was previously called "Dataset".

What is an "Asset"?
--------------------

An Airflow asset is a logical grouping of data. Upstream producer tasks can update assets, and asset updates contribute to scheduling downstream consumer Dags.

`Uniform Resource Identifier (URI) <https://en.wikipedia.org/wiki/Uniform_Resource_Identifier>`_ define assets:

.. code-block:: python

    from airflow.sdk import Asset

    example_asset = Asset("s3://asset-bucket/example.csv")

Airflow makes no assumptions about the content or location of the data represented by the URI, and treats the URI like a string. This means that Airflow treats any regular expressions, like ``input_\d+.csv``, or file glob patterns, such as ``input_2022*.csv``, as an attempt to create multiple assets from one declaration, and they will not work.

You must create assets with a valid URI. Airflow core and providers define various URI schemes that you can use, such as ``file`` (core), ``postgres`` (by the Postgres provider), and ``s3`` (by the Amazon provider). Third-party providers and plugins might also provide their own schemes. These pre-defined schemes have individual semantics that are expected to be followed. You can use the optional name argument to provide a more human-readable identifier to the asset.

.. code-block:: python

    from airflow.sdk import Asset

    example_asset = Asset(uri="s3://asset-bucket/example.csv", name="bucket-1")

What is valid URI?
------------------

Technically, the URI must conform to the valid character set in RFC 3986, which is basically ASCII alphanumeric characters, plus ``%``,  ``-``, ``_``, ``.``, and ``~``. To identify a resource that cannot be represented by URI-safe characters, encode the resource name with `percent-encoding <https://en.wikipedia.org/wiki/Percent-encoding>`_.

The URI is also case sensitive, so ``s3://example/asset`` and ``s3://Example/asset`` are considered different. Note that the *host* part of the URI is also case sensitive, which differs from RFC 3986.

For pre-defined schemes (e.g., ``file``, ``postgres``, and ``s3``), you must provide a meaning URI. If you can't provide one, use another scheme altogether that don't have the semantic restrictions. Airflow will never require a semantic for user-defined URI schemes  (with a prefix x-), so that can be a good alternative. If you have a URI that can only be obtained later (e.g., during task execution), consider using ``AssetAlias`` instead and update the URI later.

.. code-block:: python

    # invalid asset:
    must_contain_bucket_name = Asset("s3://")

Do not use the ``airflow`` scheme, which is is reserved for Airflow's internals.

Airflow always prefers using lower cases in schemes, and case sensitivity is needed in the host part of the URI to correctly distinguish between resources.

.. code-block:: python

    # invalid assets:
    reserved = Asset("airflow://example_asset")
    not_ascii = Asset("èxample_datašet")

If you want to define assets with a scheme that doesn't include additional semantic constraints, use a scheme with the prefix ``x-``. Airflow skips any semantic validation on URIs with these schemes.

.. code-block:: python

    # valid asset, treated as a plain string
    my_ds = Asset("x-my-thing://foobarbaz")

The identifier does not have to be absolute; it can be a scheme-less, relative URI, or even just a simple path or string:

.. code-block:: python

    # valid assets:
    schemeless = Asset("//example/asset")
    csv_file = Asset("example_asset")

Non-absolute identifiers are considered plain strings that do not carry any semantic meanings to Airflow.

Extra information on assets
----------------------------

If needed, you can include an additional dictionary in an asset using the ``extra`` parameter:

.. code-block:: python

    example_asset = Asset(
        "s3://asset/example.csv",
        extra={"team": "trainees"},
    )

This allows you to provide custom metadata about the asset, such as ownership information or the purpose of the file. The ``extra`` field does **NOT** affect the identity of an asset.
Thus, maintaining the uniqueness of the ``extra`` value is the user responsibility. It suggested to have only one single set of ``extra`` value per asset.

For example, in the following snippet, only one of the ``extra`` dictionaries will ultimately be stored, but it does guaranteed which one will be stored.

.. code-block:: python

     Asset("s3://asset/example.csv", extra={"d": "e"})
     Asset("s3://asset/example.csv", extra={"f": "g"})

This behavior also applies to dynamically generated assets created through ``AssetAlias``.
In the example below, the final stored ``extra`` value is not guaranteed and it might vary based on Dag processor settings.

.. code-block:: python

    from airflow.sdk import AssetAlias


    @dag(schedule=None)
    def my_dag_1():

        @task(outlets=[AssetAlias("my-task-outputs")])
        def my_task_with_outlet_events(*, outlet_events):
            outlet_events[AssetAlias("my-task-outputs")].add(
                # Asset extra set as {"from": "asset alias"}
                Asset("s3://bucket/my-task", extra={"from": "asset alias"})
            )

        my_task_with_outlet_events()


    # Asset extra set as {"key": "value"}
    @dag(schedule=Asset("s3://bucket/my-task", extra={"key": "value"}))
    def my_dag_2(): ...


    my_dag_1()
    my_dag_2()

    # It's not guaranteed which extra will be the one stored

.. note:: **Security Note:** Asset URIs and values in the ``extra`` field are stored in cleartext in Airflow's metadata database. These fields are **not encrypted**. **DO NOT** store sensitive information, especially credentials, in either the asset URI or the ``extra`` dictionary.


Creating a task to emit asset events
------------------------------------

Once an asset is defined, tasks can be created to emit events against it by specifying ``outlets``:

.. code-block:: python

    from airflow.sdk import DAG, Asset
    from airflow.providers.standard.operators.python import PythonOperator

    example_asset = Asset(name="example_asset", uri="s3://asset-bucket/example.csv")


    def _write_example_asset():
        """Write data to example_asset..."""


    with DAG(dag_id="example_asset", schedule="@daily"):
        PythonOperator(task_id="example_asset", outlets=[example_asset], python_callable=_write_example_asset)

This is quite a lot of boilerplate. Airflow provides a shorthand for this simple but most common case of *creating a Dag with one single task that emits events of one asset*. The code block below is exactly equivalent to the one above:

.. code-block:: python

    from airflow.sdk import asset


    @asset(uri="s3://asset-bucket/example.csv", schedule="@daily")
    def example_asset():
        """Write data to example_asset..."""

Declaring an ``@asset`` automatically creates:

* An ``Asset`` with *name* set to the function name.
* A ``DAG`` with *dag_id* set to the function name.
* A task inside the ``DAG`` with *task_id* set to the function name, and *outlet* to the created ``Asset``.

Attaching extra information to an emitting asset event
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.10.0

A task with an asset outlet can optionally attach extra information before it emits an asset event. This is different
from `Extra information on assets`_. Extra information on an asset statically describes the entity pointed to by the asset URI; extra information on the *asset event* instead should be used to annotate the triggering data change, such as how many rows in the database are changed by the update, or the date range covered by it.

The easiest way to attach extra information to the asset event is by ``yield``-ing a ``Metadata`` object from a task:

.. code-block:: python

    from airflow.sdk import Metadata, asset


    @asset(uri="s3://asset/example.csv", schedule=None)
    def example_s3(self):  # 'self' here refers to the current asset.
        df = ...  # Get a Pandas DataFrame to write.
        # Write df to asset...
        yield Metadata(self, {"row_count": len(df)})

Airflow automatically collects all yielded metadata, and populates asset events with extra information for corresponding metadata objects.

This can also be done in classic operators. The best way is to subclass the operator and override ``execute``. Alternatively, extras can also be added in a task's ``pre_execute`` or ``post_execute`` hook. If you choose to use hooks, however, remember that they are not rerun when a task is retried, and may cause the extra information to not match actual data in certain scenarios.

Another way to achieve the same is by accessing ``outlet_events`` in a task's execution context directly:

.. code-block:: python

    @asset(schedule=None)
    def write_to_s3(self, context):
        context["outlet_events"][self].extra = {"row_count": len(df)}

There's minimal magic here---Airflow simply writes the yielded values to the exact same accessor. This also works in classic operators, including ``execute``, ``pre_execute``, and ``post_execute``.

.. note:: Asset event extra information can only contain JSON-serializable values (list and dict nesting is possible). This is due to the value being stored in the database.

.. _fetching_information_from_previously_emitted_asset_events:

Fetching information from previously emitted asset events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.10.0

Events of an asset defined in a task's ``outlets``, as described in the previous section, can be read by a task that declares the same asset in its ``inlets``. A asset event entry contains ``extra`` (see previous section for details), ``timestamp`` indicating when the event was emitted from a task, and ``source_task_instance`` linking the event back to its source.

Inlet asset events can be read with the ``inlet_events`` accessor in the execution context. Continuing from the ``write_to_s3`` asset in the previous section:

.. code-block:: python

    @asset(schedule=None)
    def post_process_s3_file(context, write_to_s3):  # Declaring an inlet to write_to_s3.
        events = context["inlet_events"][write_to_s3]
        last_row_count = events[-1].extra["row_count"]

Each value in the ``inlet_events`` mapping is a sequence-like object that orders past events of a given asset by ``timestamp``, earliest to latest. It supports most of Python's list interface, so you can use ``[-1]`` to access the last event, ``[-2:]`` for the last two, etc. The accessor is lazy and only hits the database when you access items inside it.

Dependency between ``@asset``, ``@task``, and classic operators
---------------------------------------------------------------

Since an ``@asset`` is simply a wrapper around a Dag with a task and an asset, it is quite easy to read and ``@asset`` in a ``@task`` or a classic operator. For example, the above ``post_process_s3_file`` can also be written as a task (inside a Dag, omitted here for brevity):

.. code-block:: python

    @task(inlets=[write_to_s3])
    def post_process_s3_file(*, inlet_events):
        events = inlet_events[example_s3_asset]
        last_row_count = events[-1].extra["row_count"]


    post_process_s3_file()

The other way around also applies:

.. code-block:: python

    example_asset = Asset("example_asset")


    @task(outlets=[example_asset])
    def emit_example_asset():
        """Write to example_asset..."""


    @asset(schedule=None)
    def process_example_asset(example_asset):
        """Process inlet example_asset..."""

In addition, ``@asset`` can be used with ``@task`` to customize the task that generates the asset,
utilizing the modern TaskFlow approach described in :doc:`/tutorial/taskflow`.

This combination allows you to set initial arguments for the task and to use various operators, such as the ``BashOperator``:

.. code-block:: python

    @asset(schedule=None)
    @task.bash(retries=3)
    def example_asset():
        """Write to example_asset, from a Bash task with 3 retries..."""
        return "echo 'run'"

Output to multiple assets in one task
-------------------------------------

It is possible for a task to emit events for multiple assets. This is generally discouraged, but needed in certain situations, such as when you need to split a data source into several. This is straightforward with tasks since ``outlets`` is plural by design:

.. code-block:: python

    from airflow.sdk import DAG, Asset, task

    input_asset = Asset("input_asset")
    out_asset_1 = Asset("out_asset_1")
    out_asset_2 = Asset("out_asset_2")

    with DAG(dag_id="process_input", schedule=None):

        @task(inlets=[input_asset], outlets=[out_asset_1, out_asset_2])
        def process_input():
            """Split input into two."""

The shorthand for this is ``@asset.multi``:

.. code-block:: python

    from airflow.sdk import Asset, asset

    input_asset = Asset("input_asset")
    out_asset_1 = Asset("out_asset_1")
    out_asset_2 = Asset("out_asset_2")


    @asset.multi(schedule=None, outlets=[out_asset_1, out_asset_2])
    def process_input(input_asset):
        """Split input into two."""


Dynamic data events emitting and asset creation through AssetAlias
-----------------------------------------------------------------------
An asset alias can be used to emit asset events of assets with association to the aliases. Downstreams can depend on resolved asset. This feature allows you to define complex dependencies for Dag executions based on asset updates.

How to use AssetAlias
~~~~~~~~~~~~~~~~~~~~~~~

``AssetAlias`` has one single argument ``name`` that uniquely identifies the asset. The task must first declare the alias as an outlet, and use ``outlet_events`` or yield ``Metadata`` to add events to it.

The following example creates an asset event against the S3 URI ``f"s3://bucket/my-task"``  with optional extra information ``extra``. If the asset does not exist, Airflow will dynamically create it and log a warning message.

**Emit an asset event during task execution through outlet_events**

.. code-block:: python

    from airflow.sdk import AssetAlias


    @task(outlets=[AssetAlias("my-task-outputs")])
    def my_task_with_outlet_events(*, outlet_events):
        outlet_events[AssetAlias("my-task-outputs")].add(Asset("s3://bucket/my-task"), extra={"k": "v"})


**Emit an asset event during task execution through yielding Metadata**

.. code-block:: python

    from airflow.sdk import Metadata


    @task(outlets=[AssetAlias("my-task-outputs")])
    def my_task_with_metadata():
        s3_asset = Asset(uri="s3://bucket/my-task", name="example_s3")
        yield Metadata(s3_asset, extra={"k": "v"}, alias=AssetAlias("my-task-outputs"))

Only one asset event is emitted for an added asset, even if it is added to the alias multiple times, or added to multiple aliases. However, if different ``extra`` values are passed, it can emit multiple asset events. In the following example, two asset events will be emitted.

.. code-block:: python

    from airflow.sdk import AssetAlias


    @task(
        outlets=[
            AssetAlias("my-task-outputs-1"),
            AssetAlias("my-task-outputs-2"),
            AssetAlias("my-task-outputs-3"),
        ]
    )
    def my_task_with_outlet_events(*, outlet_events):
        outlet_events[AssetAlias("my-task-outputs-1")].add(Asset("s3://bucket/my-task"), extra={"k": "v"})
        # This line won't emit an additional asset event as the asset and extra are the same as the previous line.
        outlet_events[AssetAlias("my-task-outputs-2")].add(Asset("s3://bucket/my-task"), extra={"k": "v"})
        # This line will emit an additional asset event as the extra is different.
        outlet_events[AssetAlias("my-task-outputs-3")].add(Asset("s3://bucket/my-task"), extra={"k2": "v2"})


Fetching information from previously emitted asset events through resolved asset aliases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As mentioned in :ref:`Fetching information from previously emitted asset events<fetching_information_from_previously_emitted_asset_events>`, inlet asset events can be read with the ``inlet_events`` accessor in the execution context, and you can also use asset aliases to access the asset events triggered by them.

.. code-block:: python

    with DAG(dag_id="asset-alias-producer"):

        @task(outlets=[AssetAlias("example-alias")])
        def produce_asset_events(*, outlet_events):
            outlet_events[AssetAlias("example-alias")].add(Asset("s3://bucket/my-task"), extra={"row_count": 1})


    with DAG(dag_id="asset-alias-consumer", schedule=None):

        @task(inlets=[AssetAlias("example-alias")])
        def consume_asset_alias_events(*, inlet_events):
            events = inlet_events[AssetAlias("example-alias")]
            last_row_count = events[-1].extra["row_count"]
