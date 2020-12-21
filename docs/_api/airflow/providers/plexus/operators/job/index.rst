:mod:`airflow.providers.plexus.operators.job`
=============================================

.. py:module:: airflow.providers.plexus.operators.job


Module Contents
---------------

.. data:: logger
   

   

.. py:class:: PlexusJobOperator(job_params: Dict, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Submits a Plexus job.

   :param job_params: parameters required to launch a job.
   :type job_params: dict

   Required job parameters are the following
       - "name": job name created by user.
       - "app": name of the application to run. found in Plexus UI.
       - "queue": public cluster name. found in Plexus UI.
       - "num_nodes": number of nodes.
       - "num_cores":  number of cores per node.

   
   .. method:: execute(self, context: Any)



   
   .. method:: _api_lookup(self, param: str, hook)



   
   .. method:: construct_job_params(self, hook: Any)

      Creates job_params dict for api call to
      launch a Plexus job.

      Some parameters required to launch a job
      are not available to the user in the Plexus
      UI. For example, an app id is required, but
      only the app name is provided in the UI.
      This function acts as a backend lookup
      of the required param value using the
      user-provided value.

      :param hook: plexus hook object
      :type hook: airflow hook




