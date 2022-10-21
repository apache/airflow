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

====================
Dynamic Task Mapping
====================

Dynamic Task Mapping allows a way for a workflow to create a number of tasks at runtime based upon current data, rather than the DAG author having to know in advance how many tasks would be needed.

This is similar to defining your tasks in a for loop, but instead of having the DAG file fetch the data and do that itself, the scheduler can do this based on the output of a previous task. Right before a mapped task is executed the scheduler will create *n* copies of the task, one for each input.

It is also possible to have a task operate on the collected output of a mapped task, commonly known as map and reduce.

Simple mapping
==============

In its simplest form you can map over a list defined directly in your DAG file using the ``expand()`` function instead of calling your task directly.

.. code-block:: python

    from datetime import datetime

    from airflow import DAG
    from airflow.decorators import task


    with DAG(dag_id="simple_mapping", start_date=datetime(2022, 3, 4)) as dag:

        @task
        def add_one(x: int):
            return x + 1

        @task
        def sum_it(values):
            total = sum(values)
            print(f"Total was {total}")

        added_values = add_one.expand(x=[1, 2, 3])
        sum_it(added_values)

This will show ``Total was 9`` in the task logs when executed.

This is the resulting DAG structure:

.. image:: /img/mapping-simple-graph.png

The grid view also provides visibility into your mapped tasks in the details panel:

.. image:: /img/mapping-simple-grid.png

.. note:: Only keyword arguments are allowed to be passed to ``expand()``.

.. note:: Values passed from the mapped task is a lazy proxy

    In the above example, ``values`` received by ``sum_it`` is an aggregation of all values returned by each mapped instance of ``add_one``. However, since it is impossible to know how many instances of ``add_one`` we will have in advance, ``values`` is not a normal list, but a "lazy sequence" that retrieves each individual value only when asked. Therefore, if you run ``print(values)`` directly, you would get something like this::

        _LazyXComAccess(dag_id='simple_mapping', run_id='test_run', task_id='add_one')

    You can use normal sequence syntax on this object (e.g. ``values[0]``), or iterate through it normally with a ``for`` loop. ``list(values)`` will give you a "real" ``list``, but please be aware of the potential performance implications if the list is large.

.. note:: A reduce task is not required.

    Although we show a "reduce" task here (``sum_it``) you don't have to have one, the mapped tasks will still be executed even if they have no downstream tasks.

Repeated Mapping
================

The result of one mapped task can also be used as input to the next mapped task.

.. code-block:: python

    with DAG(dag_id="repeated_mapping", start_date=datetime(2022, 3, 4)) as dag:

        @task
        def add_one(x: int):
            return x + 1

        first = add_one.expand(x=[1, 2, 3])
        second = add_one.expand(x=first)

This would have a result of ``[3, 4, 5]``.

Constant parameters
===================

As well as passing arguments that get expanded at run-time, it is possible to pass arguments that don't change – in order to clearly differentiate between the two kinds we use different functions, ``expand()`` for mapped arguments, and ``partial()`` for unmapped ones.

.. code-block:: python

    @task
    def add(x: int, y: int):
        return x + y


    added_values = add.partial(y=10).expand(x=[1, 2, 3])
    # This results in add function being expanded to
    # add(x=1, y=10)
    # add(x=2, y=10)
    # add(x=3, y=10)

This would result in values of 11, 12, and 13.

This is also useful for passing things such as connection IDs, database table names, or bucket names to tasks.

Mapping over multiple parameters
================================

As well as a single parameter it is possible to pass multiple parameters to expand. This will have the effect of creating a "cross product", calling the mapped task with each combination of parameters.

.. code-block:: python

    @task
    def add(x: int, y: int):
        return x + y


    added_values = add.expand(x=[2, 4, 8], y=[5, 10])
    # This results in the add function being called with
    # add(x=2, y=5)
    # add(x=2, y=10)
    # add(x=4, y=5)
    # add(x=4, y=10)
    # add(x=8, y=5)
    # add(x=8, y=10)

This would result in the add task being called 6 times. Please note however that the order of expansion is not guaranteed.

Task-generated Mapping
======================

Up until now the examples we've shown could all be achieved with a ``for`` loop in the DAG file, but the real power of dynamic task mapping comes from being able to have a task generate the list to iterate over.

.. code-block:: python

    @task
    def make_list():
        # This can also be from an API call, checking a database, -- almost anything you like, as long as the
        # resulting list/dictionary can be stored in the current XCom backend.
        return [1, 2, {"a": "b"}, "str"]


    @task
    def consumer(arg):
        print(arg)


    with DAG(dag_id="dynamic-map", start_date=datetime(2022, 4, 2)) as dag:
        consumer.expand(arg=make_list())

The ``make_list`` task runs as a normal task and must return a list or dict (see `What data types can be expanded?`_), and then the ``consumer`` task will be called four times, once with each value in the return of ``make_list``.

Mapping with non-TaskFlow operators
===================================

It is possible to use ``partial`` and ``expand`` with classic style operators as well. Some arguments are not mappable and must be passed to ``partial()``, such as ``task_id``, ``queue``, ``pool``, and most other arguments to ``BaseOperator``.


.. code-block:: python

    BashOperator.partial(task_id="bash", do_xcom_push=False).expand(bash_command=["echo 1", "echo 2"])

.. note:: Only keyword arguments are allowed to be passed to ``partial()``.

Mapping over result of classic operators
----------------------------------------

If you want to map over the result of a classic operator, you should explicitly reference the *output*, instead of the operator itself.

.. code-block:: python

    # Create a list of data inputs.
    extract = ExtractOperator(task_id="extract")

    # Expand the operator to transform each input.
    transform = TransformOperator.partial(task_id="transform").expand(input=extract.output)

    # Collect the transformed inputs, expand the operator to load each one of them to the target.
    load = LoadOperator.partial(task_id="load").expand(input=transform.output)


Mixing TaskFlow and classic operators
=====================================

In this example you have a regular data delivery to an S3 bucket and want to apply the same processing to every file that arrives, no matter how many arrive each time.

.. code-block:: python

    from datetime import datetime

    from airflow import DAG
    from airflow.decorators import task
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


    with DAG(dag_id="mapped_s3", start_date=datetime(2020, 4, 7)) as dag:
        list_filenames = S3ListOperator(
            task_id="get_input",
            bucket="example-bucket",
            prefix='incoming/provider_a/{{ data_interval_start.strftime("%Y-%m-%d") }}',
        )

        @task
        def count_lines(aws_conn_id, bucket, filename):
            hook = S3Hook(aws_conn_id=aws_conn_id)

            return len(hook.read_key(filename, bucket).splitlines())

        @task
        def total(lines):
            return sum(lines)

        counts = count_lines.partial(aws_conn_id="aws_default", bucket=list_filenames.bucket).expand(
            filename=list_filenames.output
        )

        total(lines=counts)

Assigning multiple parameters to a non-TaskFlow operator
========================================================

Sometimes an upstream needs to specify multiple arguments to a downstream operator. To do this, you can use the ``expand_kwargs`` function, which takes a sequence of mappings to map against.

.. code-block:: python

    BashOperator.partial(task_id="bash").expand_kwargs(
        [
            {"bash_command": "echo $ENV1", "env": {"ENV1": "1"}},
            {"bash_command": "printf $ENV2", "env": {"ENV2": "2"}},
        ],
    )

This produces two task instances at run-time printing ``1`` and ``2`` respectively.

Similar to ``expand``, you can also map against a XCom that returns a list of dicts, or a list of XComs each returning a dict. Re-using the S3 example above, you can use a mapped task to perform "branching" and copy files to different buckets:

.. code-block:: python

    list_filenames = S3ListOperator(...)  # Same as the above example.


    @task
    def create_copy_kwargs(filename):
        if filename.rsplit(".", 1)[-1] not in ("json", "yml"):
            dest_bucket_name = "my_text_bucket"
        else:
            dest_bucket_name = "my_other_bucket"
        return {
            "source_bucket_key": filename,
            "dest_bucket_key": filename,
            "dest_bucket_name": dest_bucket_name,
        }


    copy_kwargs = create_copy_kwargs.expand(filename=list_filenames.output)

    # Copy files to another bucket, based on the file's extension.
    copy_filenames = S3CopyObjectOperator.partial(
        task_id="copy_files", source_bucket_name=list_filenames.bucket
    ).expand_kwargs(copy_kwargs)

Filtering items from an expanded task
=====================================

A mapped task can remove any elements from being passed on to its downstream tasks by returning ``None``. For example, if we want to *only* copy files from an S3 bucket to another with certain extensions, we could implement ``create_copy_kwargs`` like this instead:

.. code-block:: python

    @task
    def create_copy_kwargs(filename):
        # Skip files not ending with these suffixes.
        if filename.rsplit(".", 1)[-1] not in ("json", "yml"):
            return None
        return {
            "source_bucket_key": filename,
            "dest_bucket_key": filename,
            "dest_bucket_name": "my_other_bucket",
        }


    # copy_kwargs and copy_files are implemented the same.

This makes ``copy_files`` only expand against ``.json`` and ``.yml`` files, while ignoring the rest.

Transforming mapped data
========================

Since it is common to want to transform the output data format for task mapping, especially from a non-TaskFlow operator, where the output format is pre-determined and cannot be easily converted (such as ``create_copy_kwargs`` in the above example), a special ``map()`` function can be used to easily perform this kind of transformation. The above example can therefore be modified like this:

.. code-block:: python

    from airflow.exceptions import AirflowSkipException

    list_filenames = S3ListOperator(...)  # Unchanged.


    def create_copy_kwargs(filename):
        if filename.rsplit(".", 1)[-1] not in ("json", "yml"):
            raise AirflowSkipException(f"skipping {filename!r}; unexpected suffix")
        return {
            "source_bucket_key": filename,
            "dest_bucket_key": filename,
            "dest_bucket_name": "my_other_bucket",
        }


    copy_kwargs = list_filenames.output.map(create_copy_kwargs)

    # Unchanged.
    copy_filenames = S3CopyObjectOperator.partial(...).expand_kwargs(copy_kwargs)

There are a couple of things to note:

#. The callable argument of ``map()`` (``create_copy_kwargs`` in the example) **must not** be a task, but a plain Python function. The transformation is as a part of the "pre-processing" of the downstream task (i.e. ``copy_files``), not a standalone task in the DAG.
#. The callable always take exactly one positional argument. This function is called for each item in the iterable used for task-mapping, similar to how Python's built-in ``map()`` works.
#. Since the callable is executed as a part of the downstream task, you can use any existing techniques to write the task function. To mark a component as skipped, for example, you should raise ``AirflowSkipException``. Note that returning ``None`` **does not** work here.

Combining upstream data (aka "zipping")
=======================================

It is also to want to combine multiple input sources into one task mapping iterable. This is generally known as "zipping" (like Python's built-in ``zip()`` function), and is also performed as pre-processing of the downstream task.

This is especially useful for conditional logic in task mapping. For example, if you want to download files from S3, but rename those files, something like this would be possible:

.. code-block:: python

    list_filenames_a = S3ListOperator(
        task_id="list_files_in_a",
        bucket="bucket",
        prefix="incoming/provider_a/{{ data_interval_start|ds }}",
    )
    list_filenames_b = ["rename_1", "rename_2", "rename_3", ...]

    filenames_a_b = list_filenames_a.output.zip(list_filenames_b)


    @task
    def download_filea_from_a_rename(filenames_a_b):
        fn_a, fn_b = filenames_a_b
        S3Hook().download_file(fn_a, local_path=fn_b)


    download_filea_from_a_rename.expand(filenames_a_b=filenames_a_b)

The ``zip`` function takes arbitrary positional arguments, and return an iterable of tuples of the positional arguments' count. By default, the zipped iterable's length is the same as the shortest of the zipped iterables, with superfluous items dropped. An optional keyword argument ``default`` can be passed to switch the behavior to match Python's ``itertools.zip_longest``—the zipped iterable will have the same length as the *longest* of the zipped iterables, with missing items filled with the value provided by ``default``.

What data types can be expanded?
================================

Currently it is only possible to map against a dict, a list, or one of those types stored in XCom as the result of a task.

If an upstream task returns an unmappable type, the mapped task will fail at run-time with an ``UnmappableXComTypePushed`` exception. For instance, you can't have the upstream task return a plain string – it must be a list or a dict.

How do templated fields and mapped arguments interact?
======================================================

All arguments to an operator can be mapped, even those that do not accept templated parameters.

If a field is marked as being templated and is mapped, it **will not be templated**.

For example, this will print ``{{ ds }}`` and not a date stamp:

.. code-block:: python

    @task
    def make_list():
        return ["{{ ds }}"]


    @task
    def printer(val):
        print(val)


    printer.expand(val=make_list())

If you want to interpolate values either call ``task.render_template`` yourself, or use interpolation:

.. code-block:: python

    @task
    def make_list(ds=None):
        return [ds]


    @task
    def make_list(**context):
        return [context["task"].render_template("{{ ds }}", context)]

Placing limits on mapped tasks
==============================

There are two limits that you can place on a task:

  #. the number of mapped task instances can be created as the result of expansion.
  #. The number of the mapped task can run at once.

- **Limiting number of mapped task**

  The [core] ``max_map_length`` config option is the maximum number of tasks that ``expand`` can create – the default value is 1024.

  If a source task (``make_list`` in our earlier example) returns a list longer than this it will result in *that* task failing.

- **Limiting parallel copies of a mapped task**

  If you wish to not have a large mapped task consume all available runner slots you can use the ``max_active_tis_per_dag`` setting on the task to restrict how many can be running at the same time.

  Note however that this applies to all copies of that task against all active DagRuns, not just to this one specific DagRun.

  .. code-block:: python

      @task(max_active_tis_per_dag=16)
      def add_one(x: int):
          return x + 1


      BashOperator.partial(task_id="my_task", max_active_tis_per_dag=16).expand(bash_command=commands)

Automatically skipping zero-length maps
=======================================

If the input is empty (zero length), no new tasks will be created and the mapped task will be marked as ``SKIPPED``.
