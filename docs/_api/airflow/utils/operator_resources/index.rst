:mod:`airflow.utils.operator_resources`
=======================================

.. py:module:: airflow.utils.operator_resources


Module Contents
---------------

.. data:: MB
   :annotation: = 1

   

.. data:: GB
   

   

.. data:: TB
   

   

.. data:: PB
   

   

.. data:: EB
   

   

.. py:class:: Resource(name, units_str, qty)

   Represents a resource requirement in an execution environment for an operator.

   :param name: Name of the resource
   :type name: str
   :param units_str: The string representing the units of a resource (e.g. MB for a CPU
       resource) to be used for display purposes
   :type units_str: str
   :param qty: The number of units of the specified resource that are required for
       execution of the operator.
   :type qty: long

   .. attribute:: name
      

      Name of the resource.


   .. attribute:: units_str
      

      The string representing the units of a resource.


   .. attribute:: qty
      

      The number of units of the specified resource that are required for
      execution of the operator.


   
   .. method:: __eq__(self, other)



   
   .. method:: __repr__(self)




.. py:class:: CpuResource(qty)

   Bases: :class:`airflow.utils.operator_resources.Resource`

   Represents a CPU requirement in an execution environment for an operator.


.. py:class:: RamResource(qty)

   Bases: :class:`airflow.utils.operator_resources.Resource`

   Represents a RAM requirement in an execution environment for an operator.


.. py:class:: DiskResource(qty)

   Bases: :class:`airflow.utils.operator_resources.Resource`

   Represents a disk requirement in an execution environment for an operator.


.. py:class:: GpuResource(qty)

   Bases: :class:`airflow.utils.operator_resources.Resource`

   Represents a GPU requirement in an execution environment for an operator.


.. py:class:: Resources(cpus=conf.getint('operators', 'default_cpus'), ram=conf.getint('operators', 'default_ram'), disk=conf.getint('operators', 'default_disk'), gpus=conf.getint('operators', 'default_gpus'))

   The resources required by an operator. Resources that are not specified will use the
   default values from the airflow config.

   :param cpus: The number of cpu cores that are required
   :type cpus: long
   :param ram: The amount of RAM required
   :type ram: long
   :param disk: The amount of disk space required
   :type disk: long
   :param gpus: The number of gpu units that are required
   :type gpus: long

   
   .. method:: __eq__(self, other)



   
   .. method:: __repr__(self)




