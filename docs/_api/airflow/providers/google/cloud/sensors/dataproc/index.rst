:mod:`airflow.providers.google.cloud.sensors.dataproc`
======================================================

.. py:module:: airflow.providers.google.cloud.sensors.dataproc

.. autoapi-nested-parse::

   This module contains a Dataproc Job sensor.



Module Contents
---------------

.. py:class:: DataprocJobSensor(*, project_id: str, dataproc_job_id: str, location: str, gcp_conn_id: str = 'google_cloud_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Check for the state of a previously submitted Dataproc job.

   :param project_id: The ID of the google cloud project in which
       to create the cluster. (templated)
   :type project_id: str
   :param dataproc_job_id: The Dataproc job ID to poll. (templated)
   :type dataproc_job_id: str
   :param location: Required. The Cloud Dataproc region in which to handle the request. (templated)
   :type location: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
   :type gcp_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['project_id', 'location', 'dataproc_job_id']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: dict)




