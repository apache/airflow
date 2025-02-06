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

:py:mod:`tests.system.providers.amazon.aws.utils`
=================================================

.. py:module:: tests.system.providers.amazon.aws.utils


Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   ec2/index.rst
   k8s/index.rst


Package Contents
----------------

Classes
~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.utils.Variable
   tests.system.providers.amazon.aws.utils.SystemTestContextBuilder



Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.utils.fetch_variable
   tests.system.providers.amazon.aws.utils.set_env_id
   tests.system.providers.amazon.aws.utils.all_tasks_passed
   tests.system.providers.amazon.aws.utils.prune_logs
   tests.system.providers.amazon.aws.utils.split_string



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.utils.ENV_ID_ENVIRON_KEY
   tests.system.providers.amazon.aws.utils.ENV_ID_KEY
   tests.system.providers.amazon.aws.utils.DEFAULT_ENV_ID_PREFIX
   tests.system.providers.amazon.aws.utils.DEFAULT_ENV_ID_LEN
   tests.system.providers.amazon.aws.utils.DEFAULT_ENV_ID
   tests.system.providers.amazon.aws.utils.PURGE_LOGS_INTERVAL_PERIOD
   tests.system.providers.amazon.aws.utils.TEST_FILE_IDENTIFIER
   tests.system.providers.amazon.aws.utils.INVALID_ENV_ID_MSG
   tests.system.providers.amazon.aws.utils.LOWERCASE_ENV_ID_MSG
   tests.system.providers.amazon.aws.utils.NO_VALUE_MSG
   tests.system.providers.amazon.aws.utils.log


.. py:data:: ENV_ID_ENVIRON_KEY
   :type: str
   :value: 'SYSTEM_TESTS_ENV_ID'



.. py:data:: ENV_ID_KEY
   :type: str
   :value: 'ENV_ID'



.. py:data:: DEFAULT_ENV_ID_PREFIX
   :type: str
   :value: 'env'



.. py:data:: DEFAULT_ENV_ID_LEN
   :type: int
   :value: 8



.. py:data:: DEFAULT_ENV_ID
   :type: str



.. py:data:: PURGE_LOGS_INTERVAL_PERIOD
   :value: 5



.. py:data:: TEST_FILE_IDENTIFIER
   :type: str
   :value: 'example'



.. py:data:: INVALID_ENV_ID_MSG
   :type: str
   :value: 'In order to maximize compatibility, the SYSTEM_TESTS_ENV_ID must be an alphanumeric string which...'



.. py:data:: LOWERCASE_ENV_ID_MSG
   :type: str
   :value: 'The provided Environment ID contains uppercase letters and will be converted to lowercase for...'



.. py:data:: NO_VALUE_MSG
   :type: str
   :value: 'No Value Found: Variable {key} could not be found and no default value was provided.'



.. py:data:: log



.. py:class:: Variable(name, to_split = False, delimiter = None, test_name = None)


   Stores metadata about a variable to be fetched for AWS System Tests.

   :param name: The name of the variable to be fetched.
   :param to_split: If True, the input is a string-formatted List and needs to be split. Defaults to False.
   :param delimiter: If to_split is true, this will be used to split the string. Defaults to ','.
   :param test_name: The name of the system test that the variable is associated with.

   .. py:method:: get_value()


   .. py:method:: set_default(default)



.. py:class:: SystemTestContextBuilder


   This builder class ultimately constructs a TaskFlow task which is run at
   runtime (task execution time). This task generates and stores the test ENV_ID as well
   as any external resources requested (e.g.g IAM Roles, VPC, etc)

   .. py:method:: add_variable(variable_name, split_string = False, delimiter = None, **kwargs)

      Register a variable to fetch from environment or cloud parameter store


   .. py:method:: build()

      Build and return a TaskFlow task which will create an env_id and
      fetch requested variables. Storing everything in xcom for downstream
      tasks to use.



.. py:function:: fetch_variable(key, default_value = None, test_name = None)

   Given a Parameter name: first check for an existing Environment Variable,
   then check SSM for a value. If neither are available, fall back on the
   optional default value.

   :param key: The name of the Parameter to fetch a value for.
   :param default_value: The default value to use if no value can be found.
   :param test_name: The system test name.
   :return: The value of the parameter.


.. py:function:: set_env_id()

   Retrieves or generates an Environment ID, validate that it is suitable,
   export it as an Environment Variable, and return it.

   If an Environment ID has already been generated, use that.
   Otherwise, try to fetch it and export it as an Environment Variable.
   If there is not one available to fetch then generate one and export it as an Environment Variable.

   :return: A valid System Test Environment ID.


.. py:function:: all_tasks_passed(ti)


.. py:function:: prune_logs(logs, force_delete = False, retry = False, retry_times = 3, ti=None)

   If all tasks in this dagrun have succeeded, then delete the associated logs.
   Otherwise, append the logs with a retention policy.  This allows the logs
   to be used for troubleshooting but assures they won't build up indefinitely.

   :param logs: A list of log_group/stream_prefix tuples to delete.
   :param force_delete: Whether to check log streams within the log group before
       removal. If True, removes the log group and all its log streams inside it.
   :param retry: Whether to retry if the log group/stream was not found. In some
       cases, the log group/stream is created seconds after the main resource has
       been created. By default, it retries for 3 times with a 5s waiting period.
   :param retry_times: Number of retries.
   :param ti: Used to check the status of the tasks. This gets pulled from the
       DAG's context and does not need to be passed manually.


.. py:function:: split_string(string)
