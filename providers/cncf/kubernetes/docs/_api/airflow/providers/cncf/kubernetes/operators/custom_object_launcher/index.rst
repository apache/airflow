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

airflow.providers.cncf.kubernetes.operators.custom_object_launcher
==================================================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.custom_object_launcher

.. autoapi-nested-parse::

   Launches Custom object.



Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkJobSpec
   airflow.providers.cncf.kubernetes.operators.custom_object_launcher.KubernetesSpec
   airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkResources
   airflow.providers.cncf.kubernetes.operators.custom_object_launcher.CustomObjectStatus
   airflow.providers.cncf.kubernetes.operators.custom_object_launcher.CustomObjectLauncher


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.custom_object_launcher.should_retry_start_spark_job


Module Contents
---------------

.. py:function:: should_retry_start_spark_job(exception)

   Check if an Exception indicates a transient error and warrants retrying.


.. py:class:: SparkJobSpec(**entries)

   Spark job spec.


   .. py:method:: validate()


   .. py:method:: update_resources()


.. py:class:: KubernetesSpec(**entries)

   Spark kubernetes spec.


   .. py:method:: set_attribute()


.. py:class:: SparkResources(driver = None, executor = None)

   spark resources.


   .. py:attribute:: default


   .. py:attribute:: driver


   .. py:attribute:: executor


   .. py:property:: resources

      Return job resources.



   .. py:property:: driver_resources

      Return resources to use.



   .. py:property:: executor_resources

      Return resources to use.



   .. py:method:: convert_resources()


.. py:class:: CustomObjectStatus

   Status of the PODs.


   .. py:attribute:: SUBMITTED
      :value: 'SUBMITTED'



   .. py:attribute:: RUNNING
      :value: 'RUNNING'



   .. py:attribute:: FAILED
      :value: 'FAILED'



   .. py:attribute:: SUCCEEDED
      :value: 'SUCCEEDED'



.. py:class:: CustomObjectLauncher(name, namespace, kube_client, custom_obj_api, template_body = None)

   Bases: :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`


   Launches PODS.


   .. py:attribute:: name


   .. py:attribute:: namespace


   .. py:attribute:: template_body
      :value: None



   .. py:attribute:: body
      :type:  dict


   .. py:attribute:: kind


   .. py:attribute:: plural
      :value: 'Uninferables'



   .. py:attribute:: custom_obj_api


   .. py:attribute:: spark_obj_spec
      :type:  dict


   .. py:attribute:: pod_spec
      :type:  kubernetes.client.models.V1Pod | None
      :value: None



   .. py:property:: pod_manager
      :type: airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager



   .. py:method:: get_body()


   .. py:method:: start_spark_job(image=None, code_path=None, startup_timeout = 600)

      Launch the pod synchronously and waits for completion.

      :param image: image name
      :param code_path: path to the .py file for python and jar file for scala
      :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
      :return:



   .. py:method:: spark_job_not_running(spark_obj_spec)

      Test if spark_obj_spec has not started.



   .. py:method:: check_pod_start_failure()


   .. py:method:: delete_spark_job(spark_job_name=None)

      Delete spark job.
