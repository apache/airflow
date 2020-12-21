:mod:`airflow.providers.amazon.aws.operators.emr_create_job_flow`
=================================================================

.. py:module:: airflow.providers.amazon.aws.operators.emr_create_job_flow


Module Contents
---------------

.. py:class:: EmrCreateJobFlowOperator(*, aws_conn_id: str = 'aws_default', emr_conn_id: str = 'emr_default', job_flow_overrides: Optional[Union[str, Dict[str, Any]]] = None, region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates an EMR JobFlow, reading the config from the EMR connection.
   A dictionary of JobFlow overrides can be passed that override
   the config from the connection.

   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str
   :param emr_conn_id: emr connection to use
   :type emr_conn_id: str
   :param job_flow_overrides: boto3 style arguments or reference to an arguments file
       (must be '.json') to override emr_connection extra. (templated)
   :type job_flow_overrides: dict|str

   .. attribute:: template_fields
      :annotation: = ['job_flow_overrides']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   .. attribute:: ui_color
      :annotation: = #f9c915

      

   
   .. method:: execute(self, context: Dict[str, Any])




