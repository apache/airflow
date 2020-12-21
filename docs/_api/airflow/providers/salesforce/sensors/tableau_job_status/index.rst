:mod:`airflow.providers.salesforce.sensors.tableau_job_status`
==============================================================

.. py:module:: airflow.providers.salesforce.sensors.tableau_job_status


Module Contents
---------------

.. py:exception:: TableauJobFailedException

   Bases: :class:`airflow.exceptions.AirflowException`

   An exception that indicates that a Job failed to complete.


.. py:class:: TableauJobStatusSensor(*, job_id: str, site_id: Optional[str] = None, tableau_conn_id: str = 'tableau_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Watches the status of a Tableau Server Job.

   .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#jobs

   :param job_id: The job to watch.
   :type job_id: str
   :param site_id: The id of the site where the workbook belongs to.
   :type site_id: Optional[str]
   :param tableau_conn_id: The Tableau Connection id containing the credentials
       to authenticate to the Tableau Server.
   :type tableau_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['job_id']

      

   
   .. method:: poke(self, context: dict)

      Pokes until the job has successfully finished.

      :param context: The task context during execution.
      :type context: dict
      :return: True if it succeeded and False if not.
      :rtype: bool




