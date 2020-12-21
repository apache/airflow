:mod:`airflow.lineage`
======================

.. py:module:: airflow.lineage

.. autoapi-nested-parse::

   Provides lineage support functions



Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   entities/index.rst


Package Contents
----------------

.. py:class:: Operator

   Class just used for Typing


.. function:: import_string(dotted_path)
   Import a dotted module path and return the attribute/class designated by the
   last name in the path. Raise ImportError if the import failed.


.. data:: ENV
   

   

.. data:: PIPELINE_OUTLETS
   :annotation: = pipeline_outlets

   

.. data:: PIPELINE_INLETS
   :annotation: = pipeline_inlets

   

.. data:: AUTO
   :annotation: = auto

   

.. data:: log
   

   

.. py:class:: Metadata

   Class for serialized entities.

   .. attribute:: type_name
      :annotation: :str

      

   .. attribute:: source
      :annotation: :str

      

   .. attribute:: data
      :annotation: :Dict

      


.. function:: _get_instance(meta: Metadata)
   Instantiate an object from Metadata


.. function:: _render_object(obj: Any, context) -> Any
   Renders a attr annotated object. Will set non serializable attributes to none


.. function:: _to_dataset(obj: Any, source: str) -> Optional[Metadata]
   Create Metadata from attr annotated object


.. data:: T
   

   

.. function:: apply_lineage(func: T) -> T
   Saves the lineage to XCom and if configured to do so sends it
   to the backend.


.. function:: prepare_lineage(func: T) -> T
   Prepares the lineage inlets and outlets. Inlets can be:

   * "auto" -> picks up any outlets from direct upstream tasks that have outlets defined, as such that
     if A -> B -> C and B does not have outlets but A does, these are provided as inlets.
   * "list of task_ids" -> picks up outlets from the upstream task_ids
   * "list of datasets" -> manually defined list of data


