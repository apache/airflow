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

Error Tracking
===============

Airflow can be set up to send errors to `Sentry <https://docs.sentry.io/>`__.

Setup
------

First you must install sentry requirement:

.. code-block:: bash

   pip install 'apache-airflow[sentry]'

After that, you need to enable the integration by setting the ``sentry_on`` option in the ``[sentry]`` section to ``True``.

Add your ``SENTRY_DSN`` to your configuration file e.g. ``airflow.cfg`` in ``[sentry]`` section. Its template resembles the following: ``{PROTOCOL}://{PUBLIC_KEY}@{HOST}/{PROJECT_ID}``

.. code-block:: ini

    [sentry]
    sentry_on = True
    sentry_dsn = http://foo@sentry.io/123

.. note::
    If this value is not provided, the SDK will try to read it from the ``SENTRY_DSN`` environment variable.

The ``before_send`` option can be used to modify or drop events before they are sent to Sentry. To set this option, provide a dotted path to a before_send function that the sentry SDK should be configured to use.

.. code-block:: ini

    [sentry]
    before_send = path.to.my.sentry.before_send

The ``transport`` option can be used to change the transport used to send events to Sentry, and possibly other Systems. To set this option, provide a dotted path to a Transport class that the sentry SDK should be configured to use.

.. code-block:: ini

    [sentry]
    transport = path.to.my.sentry.Transport

You can supply `additional configuration options <https://docs.sentry.io/platforms/python/configuration/options>`__ based on the Python platform via ``[sentry]`` section. Unsupported options: ``integrations``, ``in_app_include``, ``in_app_exclude``, ``ignore_errors``, ``before_breadcrumb``.

Tags
-----

======================================= ==================================================
Name                                    Description
======================================= ==================================================
``dag_id``                              Dag name of the Dag that failed
``task_id``                             Task name of the task that failed
``data_interval_start``                 Start of data interval when the task failed
``data_interval_end``                   End of data interval when the task failed
``operator``                            Operator name of the task that failed
======================================= ==================================================

For backward compatibility, an additional tag ``execution_date`` is also
available to represent the logical date. The tag should be considered deprecated
in favor of ``data_interval_start``.


Breadcrumbs
------------

When a task fails with an error `breadcrumbs <https://docs.sentry.io/platforms/python/enriching-events/breadcrumbs/>`__ will be added for the other tasks in the current Dag run.

======================================= ==============================================================
Name                                    Description
======================================= ==============================================================
``completed_tasks[task_id]``            Task ID of task that executed before failed task
``completed_tasks[state]``              Final state of task that executed before failed task (only Success and Failed states are captured)
``completed_tasks[operator]``           Task operator of task that executed before failed task
``completed_tasks[duration]``           Duration in seconds of task that executed before failed task
======================================= ==============================================================


Impact of Sentry on Environment variables passed to Subprocess Hook
-------------------------------------------------------------------

When Sentry is enabled, by default it changes the standard library to pass all environment variables to
subprocesses opened by Airflow. This changes the default behaviour of
:class:`airflow.providers.standard.hooks.subprocess.SubprocessHook` - always all environment variables are passed to the
subprocess executed with specific set of environment variables. In this case not only the specified
environment variables are passed but also all existing environment variables are passed with
``SUBPROCESS_`` prefix added. This happens also for all other subprocesses.

This behaviour can be disabled by setting ``default_integrations`` sentry configuration parameter to
``False`` which disables ``StdlibIntegration``. However, this also disables other default integrations,
so you need to enable them manually if you want them to remain enabled
(see `Sentry Default Integrations <https://docs.sentry.io/platforms/python/configuration/integrations/default-integrations/>`_).

.. code-block:: ini

    [sentry]
    default_integrations = False
