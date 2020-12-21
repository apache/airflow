:mod:`airflow.models.dagcode`
=============================

.. py:module:: airflow.models.dagcode


Module Contents
---------------

.. data:: log
   

   

.. py:class:: DagCode(full_filepath: str, source_code: Optional[str] = None)

   Bases: :class:`airflow.models.base.Base`

   A table for DAGs code.

   dag_code table contains code of DAG files synchronized by scheduler.
   This feature is controlled by:

   * ``[core] store_dag_code = True``: enable this feature

   For details on dag serialization see SerializedDagModel

   .. attribute:: __tablename__
      :annotation: = dag_code

      

   .. attribute:: fileloc_hash
      

      

   .. attribute:: fileloc
      

      

   .. attribute:: last_updated
      

      

   .. attribute:: source_code
      

      

   
   .. method:: sync_to_db(self, session=None)

      Writes code into database.

      :param session: ORM Session



   
   .. classmethod:: bulk_sync_to_db(cls, filelocs: Iterable[str], session=None)

      Writes code in bulk into database.

      :param filelocs: file paths of DAGs to sync
      :param session: ORM Session



   
   .. classmethod:: remove_deleted_code(cls, alive_dag_filelocs: List[str], session=None)

      Deletes code not included in alive_dag_filelocs.

      :param alive_dag_filelocs: file paths of alive DAGs
      :param session: ORM Session



   
   .. classmethod:: has_dag(cls, fileloc: str, session=None)

      Checks a file exist in dag_code table.

      :param fileloc: the file to check
      :param session: ORM Session



   
   .. classmethod:: get_code_by_fileloc(cls, fileloc: str)

      Returns source code for a given fileloc.

      :param fileloc: file path of a DAG
      :return: source code as string



   
   .. classmethod:: code(cls, fileloc)

      Returns source code for this DagCode object.

      :return: source code as string



   
   .. staticmethod:: _get_code_from_file(fileloc)



   
   .. classmethod:: _get_code_from_db(cls, fileloc, session=None)



   
   .. staticmethod:: dag_fileloc_hash(full_filepath: str)

      Hashing file location for indexing.

      :param full_filepath: full filepath of DAG file
      :return: hashed full_filepath




