:mod:`airflow.models.xcom_arg`
==============================

.. py:module:: airflow.models.xcom_arg


Module Contents
---------------

.. py:class:: XComArg(operator: BaseOperator, key: str = XCOM_RETURN_KEY)

   Bases: :class:`airflow.models.taskmixin.TaskMixin`

   Class that represents a XCom push from a previous operator.
   Defaults to "return_value" as only key.

   Current implementation supports
       xcomarg >> op
       xcomarg << op
       op >> xcomarg   (by BaseOperator code)
       op << xcomarg   (by BaseOperator code)

   **Example**: The moment you get a result from any operator (decorated or regular) you can ::

       any_op = AnyOperator()
       xcomarg = XComArg(any_op)
       # or equivalently
       xcomarg = any_op.output
       my_op = MyOperator()
       my_op >> xcomarg

   This object can be used in legacy Operators via Jinja.

   **Example**: You can make this result to be part of any generated string ::

       any_op = AnyOperator()
       xcomarg = any_op.output
       op1 = MyOperator(my_text_message=f"the value is {xcomarg}")
       op2 = MyOperator(my_text_message=f"the value is {xcomarg['topic']}")

   :param operator: operator to which the XComArg belongs to
   :type operator: airflow.models.baseoperator.BaseOperator
   :param key: key value which is used for xcom_pull (key in the XCom table)
   :type key: str

   .. attribute:: operator
      

      Returns operator of this XComArg.


   .. attribute:: roots
      

      Required by TaskMixin


   .. attribute:: leaves
      

      Required by TaskMixin


   .. attribute:: key
      

      Returns keys of this XComArg


   
   .. method:: __eq__(self, other)



   
   .. method:: __getitem__(self, item)

      Implements xcomresult['some_result_key']



   
   .. method:: __str__(self)

      Backward compatibility for old-style jinja used in Airflow Operators

      **Example**: to use XComArg at BashOperator::

          BashOperator(cmd=f"... { xcomarg } ...")

      :return:



   
   .. method:: set_upstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]])

      Proxy to underlying operator set_upstream method. Required by TaskMixin.



   
   .. method:: set_downstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]])

      Proxy to underlying operator set_downstream method. Required by TaskMixin.



   
   .. method:: resolve(self, context: Dict)

      Pull XCom value for the existing arg. This method is run during ``op.execute()``
      in respectable context.




