:mod:`airflow.providers.amazon.aws.operators.emr_add_steps`
===========================================================

.. py:module:: airflow.providers.amazon.aws.operators.emr_add_steps


Module Contents
---------------

.. py:class:: EmrAddStepsOperator(*, job_flow_id: Optional[str] = None, job_flow_name: Optional[str] = None, cluster_states: Optional[List[str]] = None, aws_conn_id: str = 'aws_default', steps: Optional[Union[List[dict], str]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   An operator that adds steps to an existing EMR job_flow.

   :param job_flow_id: id of the JobFlow to add steps to. (templated)
   :type job_flow_id: Optional[str]
   :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
       job_flow_id. will search for id of JobFlow with matching name in one of the states in
       param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
   :type job_flow_name: Optional[str]
   :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
       (templated)
   :type cluster_states: list
   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str
   :param steps: boto3 style steps or reference to a steps file (must be '.json') to
       be added to the jobflow. (templated)
   :type steps: list|str
   :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
   :type do_xcom_push: bool

   .. attribute:: template_fields
      :annotation: = ['job_flow_id', 'job_flow_name', 'cluster_states', 'steps']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   .. attribute:: ui_color
      :annotation: = #f9c915

      

   
   .. method:: execute(self, context: Dict[str, Any])




