:mod:`airflow.models.taskmixin`
===============================

.. py:module:: airflow.models.taskmixin


Module Contents
---------------

.. py:class:: TaskMixin

   Mixing implementing common chain methods like >> and <<.

   In the following functions we use:
   Task = Union[BaseOperator, XComArg]
   No type annotations due to cyclic imports.

   .. attribute:: roots
      

      Should return list of root operator List[BaseOperator]


   .. attribute:: leaves
      

      Should return list of leaf operator List[BaseOperator]


   
   .. method:: set_upstream(self, other: Union['TaskMixin', Sequence['TaskMixin']])

      Set a task or a task list to be directly upstream from the current task.



   
   .. method:: set_downstream(self, other: Union['TaskMixin', Sequence['TaskMixin']])

      Set a task or a task list to be directly downstream from the current task.



   
   .. method:: update_relative(self, other: 'TaskMixin', upstream=True)

      Update relationship information about another TaskMixin. Default is no-op.
      Override if necessary.



   
   .. method:: __lshift__(self, other: Union['TaskMixin', Sequence['TaskMixin']])

      Implements Task << Task



   
   .. method:: __rshift__(self, other: Union['TaskMixin', Sequence['TaskMixin']])

      Implements Task >> Task



   
   .. method:: __rrshift__(self, other: Union['TaskMixin', Sequence['TaskMixin']])

      Called for Task >> [Task] because list don't have __rshift__ operators.



   
   .. method:: __rlshift__(self, other: Union['TaskMixin', Sequence['TaskMixin']])

      Called for Task << [Task] because list don't have __lshift__ operators.




