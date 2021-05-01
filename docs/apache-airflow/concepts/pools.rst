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

Pools
=====

Some systems can get overwhelmed when too many processes hit them at the same time. Airflow pools can be used to **limit the execution parallelism** on arbitrary sets of tasks. The list of pools is managed in the UI (``Menu -> Admin -> Pools``) by giving the pools a name and assigning it a number of worker slots.

Tasks can then be associated with one of the existing pools by using the ``pool`` parameter when creating tasks:

.. code-block:: python

    aggregate_db_message_job = BashOperator(
        task_id='aggregate_db_message_job',
        execution_timeout=timedelta(hours=3),
        pool='ep_data_pipeline_db_msg_agg',
        bash_command=aggregate_db_message_job_cmd,
        dag=dag,
    )
    aggregate_db_message_job.set_upstream(wait_for_empty_queue)

The ``pool`` parameter can be used in conjunction with the ``priority_weight`` parameter to define priorities in the queue, and which tasks get executed first as slots open up in the pool.

The default ``priority_weight`` is ``1``, and can be bumped to any number. When sorting the queue to evaluate which task should be executed next, we use the ``priority_weight``, summed up with all of the ``priority_weight`` values from tasks downstream from this task; the highest summed value wins. Thus, you can bump a specific important task, and the whole path to that task gets prioritized accordingly.

Tasks will be scheduled as usual while the slots fill up. Once capacity is reached, runnable tasks get queued and their state will show as such in the UI. As slots free up, queued tasks start running based on the ``priority_weight`` (of the task and its descendants).

Note that if tasks are not given a pool, they are assigned to a default pool ``default_pool``.  ``default_pool`` is initialized with 128 slots and can be modified through the UI or CLI (but cannot be removed).

.. warning::

    Pools and SubDAGs do not interact as you might first expect. SubDAGs will *not* honor any pool you set on them at the top level; pools must be set on the tasks *inside* the SubDAG directly.
