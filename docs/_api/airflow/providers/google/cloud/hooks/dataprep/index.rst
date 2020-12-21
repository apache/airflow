:mod:`airflow.providers.google.cloud.hooks.dataprep`
====================================================

.. py:module:: airflow.providers.google.cloud.hooks.dataprep

.. autoapi-nested-parse::

   This module contains Google Dataprep hook.



Module Contents
---------------

.. py:class:: GoogleDataprepHook(dataprep_conn_id: str = 'dataprep_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook for connection with Dataprep API.
   To get connection Dataprep with Airflow you need Dataprep token.
   https://clouddataprep.com/documentation/api#section/Authentication

   It should be added to the Connection in Airflow in JSON format.

   .. attribute:: _headers
      

      

   
   .. method:: get_jobs_for_job_group(self, job_id: int)

      Get information about the batch jobs within a Cloud Dataprep job.

      :param job_id: The ID of the job that will be fetched
      :type job_id: int



   
   .. method:: get_job_group(self, job_group_id: int, embed: str, include_deleted: bool)

      Get the specified job group.
      A job group is a job that is executed from a specific node in a flow.

      :param job_group_id: The ID of the job that will be fetched
      :type job_group_id: int
      :param embed: Comma-separated list of objects to pull in as part of the response
      :type embed: str
      :param include_deleted: if set to "true", will include deleted objects
      :type include_deleted: bool



   
   .. method:: run_job_group(self, body_request: dict)

      Creates a ``jobGroup``, which launches the specified job as the authenticated user.
      This performs the same action as clicking on the Run Job button in the application.
      To get recipe_id please follow the Dataprep API documentation
      https://clouddataprep.com/documentation/api#operation/runJobGroup

      :param body_request: The identifier for the recipe you would like to run.
      :type body_request: dict



   
   .. method:: _raise_for_status(self, response: requests.models.Response)




