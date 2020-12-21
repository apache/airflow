:mod:`airflow.providers.amazon.aws.operators.emr_terminate_job_flow`
====================================================================

.. py:module:: airflow.providers.amazon.aws.operators.emr_terminate_job_flow


Module Contents
---------------

.. py:class:: EmrTerminateJobFlowOperator(*, job_flow_id: str, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator to terminate EMR JobFlows.

   :param job_flow_id: id of the JobFlow to terminate. (templated)
   :type job_flow_id: str
   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['job_flow_id']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #f9c915

      

   
   .. method:: execute(self, context: Dict[str, Any])




