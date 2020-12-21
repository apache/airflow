:mod:`airflow.utils.dot_renderer`
=================================

.. py:module:: airflow.utils.dot_renderer

.. autoapi-nested-parse::

   Renderer DAG (tasks and dependencies) to the graphviz object.



Module Contents
---------------

.. function:: _refine_color(color: str)
   Converts color in #RGB (12 bits) format to #RRGGBB (32 bits), if it possible.
   Otherwise, it returns the original value. Graphviz does not support colors in #RGB format.

   :param color: Text representation of color
   :return: Refined representation of color


.. function:: render_dag(dag: DAG, tis: Optional[List[TaskInstance]] = None) -> graphviz.Digraph
   Renders the DAG object to the DOT object.

   If an task instance list is passed, the nodes will be painted according to task statuses.

   :param dag: DAG that will be rendered.
   :type dag: airflow.models.dag.DAG
   :param tis: List of task instances
   :type tis: Optional[List[TaskInstance]]
   :return: Graphviz object
   :rtype: graphviz.Digraph


