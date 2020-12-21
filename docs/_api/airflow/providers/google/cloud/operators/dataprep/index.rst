:mod:`airflow.providers.google.cloud.operators.dataprep`
========================================================

.. py:module:: airflow.providers.google.cloud.operators.dataprep

.. autoapi-nested-parse::

   This module contains a Google Dataprep operator.



Module Contents
---------------

.. py:class:: DataprepGetJobsForJobGroupOperator(*, dataprep_conn_id: str = 'dataprep_default', job_id: int, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Get information about the batch jobs within a Cloud Dataprep job.
   API documentation https://clouddataprep.com/documentation/api#section/Overview

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DataprepGetJobsForJobGroupOperator`

   :param job_id The ID of the job that will be requests
   :type job_id: int

   .. attribute:: template_fields
      :annotation: = ['job_id']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: DataprepGetJobGroupOperator(*, dataprep_conn_id: str = 'dataprep_default', job_group_id: int, embed: str, include_deleted: bool, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Get the specified job group.
   A job group is a job that is executed from a specific node in a flow.
   API documentation https://clouddataprep.com/documentation/api#section/Overview

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DataprepGetJobGroupOperator`

   :param job_group_id: The ID of the job that will be requests
   :type job_group_id: int
   :param embed: Comma-separated list of objects to pull in as part of the response
   :type embed: string
   :param include_deleted: if set to "true", will include deleted objects
   :type include_deleted: bool

   .. attribute:: template_fields
      :annotation: = ['job_group_id', 'embed']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: DataprepRunJobGroupOperator(*, dataprep_conn_id: str = 'dataprep_default', body_request: dict, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Create a ``jobGroup``, which launches the specified job as the authenticated user.
   This performs the same action as clicking on the Run Job button in the application.
   To get recipe_id please follow the Dataprep API documentation
   https://clouddataprep.com/documentation/api#operation/runJobGroup

   :param recipe_id: The identifier for the recipe you would like to run.
   :type recipe_id: int

   .. attribute:: template_fields
      :annotation: = ['body_request']

      

   
   .. method:: execute(self, context: None)




