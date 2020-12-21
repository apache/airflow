:mod:`airflow.providers.papermill.operators.papermill`
======================================================

.. py:module:: airflow.providers.papermill.operators.papermill


Module Contents
---------------

.. py:class:: NoteBook

   Bases: :class:`airflow.lineage.entities.File`

   Jupyter notebook

   .. attribute:: type_hint
      :annotation: :Optional[str] = jupyter_notebook

      

   .. attribute:: parameters
      :annotation: :Optional[Dict]

      

   .. attribute:: meta_schema
      :annotation: :str

      


.. py:class:: PapermillOperator(*, input_nb: Optional[str] = None, output_nb: Optional[str] = None, parameters: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes a jupyter notebook through papermill that is annotated with parameters

   :param input_nb: input notebook (can also be a NoteBook or a File inlet)
   :type input_nb: str
   :param output_nb: output notebook (can also be a NoteBook or File outlet)
   :type output_nb: str
   :param parameters: the notebook parameters to set
   :type parameters: dict

   .. attribute:: supports_lineage
      :annotation: = True

      

   
   .. method:: execute(self, context)




