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

airflow.providers.cncf.kubernetes.kubernetes_helper_functions
=============================================================

.. py:module:: airflow.providers.cncf.kubernetes.kubernetes_helper_functions


Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.log
   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.alphanum_lower
   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.POD_NAME_MAX_LENGTH


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.create_unique_id
   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.annotations_to_key
   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.get_logs_task_metadata
   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.annotations_for_logging_task_metadata
   airflow.providers.cncf.kubernetes.kubernetes_helper_functions.should_retry_creation


Module Contents
---------------

.. py:data:: log

.. py:data:: alphanum_lower
   :value: 'abcdefghijklmnopqrstuvwxyz0123456789'


.. py:data:: POD_NAME_MAX_LENGTH
   :value: 63


.. py:function:: create_unique_id(dag_id = None, task_id = None, *, max_length = POD_NAME_MAX_LENGTH, unique = True)

   Generate unique pod or job ID given a dag_id and / or task_id.

   :param dag_id: DAG ID
   :param task_id: Task ID
   :param max_length: max number of characters
   :param unique: whether a random string suffix should be added
   :return: A valid identifier for a kubernetes pod name


.. py:function:: annotations_to_key(annotations)

   Build a TaskInstanceKey based on pod annotations.


.. py:function:: get_logs_task_metadata()

.. py:function:: annotations_for_logging_task_metadata(annotation_set)

.. py:function:: should_retry_creation(exception)

   Check if an Exception indicates a transient error and warrants retrying.

   This function is needed for preventing 'No agent available' error. The error appears time to time
   when users try to create a Resource or Job. This issue is inside kubernetes and in the current moment
   has no solution. Like a temporary solution we decided to retry Job or Resource creation request each
   time when this error appears.
   More about this issue here: https://github.com/cert-manager/cert-manager/issues/6457
