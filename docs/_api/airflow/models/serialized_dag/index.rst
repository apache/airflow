:mod:`airflow.models.serialized_dag`
====================================

.. py:module:: airflow.models.serialized_dag

.. autoapi-nested-parse::

   Serialized DAG table in database.



Module Contents
---------------

.. data:: log
   

   

.. py:class:: SerializedDagModel(dag: DAG)

   Bases: :class:`airflow.models.base.Base`

   A table for serialized DAGs.

   serialized_dag table is a snapshot of DAG files synchronized by scheduler.
   This feature is controlled by:

   * ``[core] min_serialized_dag_update_interval = 30`` (s):
     serialized DAGs are updated in DB when a file gets processed by scheduler,
     to reduce DB write rate, there is a minimal interval of updating serialized DAGs.
   * ``[scheduler] dag_dir_list_interval = 300`` (s):
     interval of deleting serialized DAGs in DB when the files are deleted, suggest
     to use a smaller interval such as 60

   It is used by webserver to load dags
   because reading from database is lightweight compared to importing from files,
   it solves the webserver scalability issue.

   .. attribute:: __tablename__
      :annotation: = serialized_dag

      

   .. attribute:: dag_id
      

      

   .. attribute:: fileloc
      

      

   .. attribute:: fileloc_hash
      

      

   .. attribute:: data
      

      

   .. attribute:: last_updated
      

      

   .. attribute:: dag_hash
      

      

   .. attribute:: __table_args__
      

      

   .. attribute:: dag_runs
      

      

   .. attribute:: dag_model
      

      

   .. attribute:: dag
      

      The DAG deserialized from the ``data`` column


   
   .. method:: __repr__(self)



   
   .. classmethod:: write_dag(cls, dag: DAG, min_update_interval: Optional[int] = None, session: Session = None)

      Serializes a DAG and writes it into database.
      If the record already exists, it checks if the Serialized DAG changed or not. If it is
      changed, it updates the record, ignores otherwise.

      :param dag: a DAG to be written into database
      :param min_update_interval: minimal interval in seconds to update serialized DAG
      :param session: ORM Session



   
   .. classmethod:: read_all_dags(cls, session: Session = None)

      Reads all DAGs in serialized_dag table.

      :param session: ORM Session
      :returns: a dict of DAGs read from database



   
   .. classmethod:: remove_dag(cls, dag_id: str, session: Session = None)

      Deletes a DAG with given dag_id.

      :param dag_id: dag_id to be deleted
      :param session: ORM Session



   
   .. classmethod:: remove_deleted_dags(cls, alive_dag_filelocs: List[str], session=None)

      Deletes DAGs not included in alive_dag_filelocs.

      :param alive_dag_filelocs: file paths of alive DAGs
      :param session: ORM Session



   
   .. classmethod:: has_dag(cls, dag_id: str, session: Session = None)

      Checks a DAG exist in serialized_dag table.

      :param dag_id: the DAG to check
      :param session: ORM Session



   
   .. classmethod:: get(cls, dag_id: str, session: Session = None)

      Get the SerializedDAG for the given dag ID.
      It will cope with being passed the ID of a subdag by looking up the
      root dag_id from the DAG table.

      :param dag_id: the DAG to fetch
      :param session: ORM Session



   
   .. staticmethod:: bulk_sync_to_db(dags: List[DAG], session: Session = None)

      Saves DAGs as Serialized DAG objects in the database. Each
      DAG is saved in a separate database query.

      :param dags: the DAG objects to save to the DB
      :type dags: List[airflow.models.dag.DAG]
      :param session: ORM Session
      :type session: Session
      :return: None



   
   .. classmethod:: get_last_updated_datetime(cls, dag_id: str, session: Session = None)

      Get the date when the Serialized DAG associated to DAG was last updated
      in serialized_dag table

      :param dag_id: DAG ID
      :type dag_id: str
      :param session: ORM Session
      :type session: Session



   
   .. classmethod:: get_latest_version_hash(cls, dag_id: str, session: Session = None)

      Get the latest DAG version for a given DAG ID.

      :param dag_id: DAG ID
      :type dag_id: str
      :param session: ORM Session
      :type session: Session
      :return: DAG Hash
      :rtype: str




