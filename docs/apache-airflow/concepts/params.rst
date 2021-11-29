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

Params
======

Params are Airflow's concept of providing runtime configuration to tasks when a dag gets triggered manually.
Params are configured while defining the dag & tasks, that can be altered while doing a manual trigger. The
ability to update params while triggering a DAG depends on the flag ``core.dag_run_conf_overrides_params``,
so if that flag is ``False``, params would behave like constants.

To use them, one can use the ``Param`` class for complex trigger-time validations or simply use primitive types,
which won't be doing any such validations.

.. code-block::

    from airflow import DAG
    from airflow.models.param import Param

    with DAG(
        'my_dag',
        params={
            'int_param': Param(10, type='integer', minimum=0, maximum=20),  # a int param with default value
            'str_param': Param(type='string', minLength=2, maxLength=4),    # a mandatory str param
            'dummy_param': Param(type=['null', 'number', 'string'])         # a param which can be None as well
            'old_param': 'old_way_of_passing',                              # i.e. no data or type validations
            'simple_param': Param('im_just_like_old_param'),                # i.e. no data or type validations
            'email_param': Param(
                default='example@example.com',
                type='string',
                format='idn-email',
                minLength=5,
                maxLength=255,
            ),
        },
    )

``Param`` make use of `json-schema <https://json-schema.org/>`__ to define the properties and doing the
validation, so one can use the full json-schema specifications mentioned at
https://json-schema.org/draft/2020-12/json-schema-validation.html to define the construct of a ``Param``
objects.

Also, it worthwhile to note that if you have any DAG which uses a mandatory param value, i.e. a ``Param``
object with no default value or ``null`` as an allowed type, that DAG schedule has to be ``None``. However,
if such ``Param`` has been defined at task level, Airflow has no way to restrict that & the task would be
failing at the execution time.

.. note::
    As of now, for security reasons, one can not use Param objects derived out of custom classes. We are
    planning to have a registration system for custom Param classes, just like we've for Operator ExtraLinks.
