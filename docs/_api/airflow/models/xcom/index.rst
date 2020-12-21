:mod:`airflow.models.xcom`
==========================

.. py:module:: airflow.models.xcom


Module Contents
---------------

.. data:: log
   

   

.. data:: MAX_XCOM_SIZE
   :annotation: = 49344

   

.. data:: XCOM_RETURN_KEY
   :annotation: = return_value

   

.. py:class:: BaseXCom

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Base class for XCom objects.

   .. attribute:: __tablename__
      :annotation: = xcom

      

   .. attribute:: key
      

      

   .. attribute:: value
      

      

   .. attribute:: timestamp
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   
   .. method:: init_on_load(self)

      Called by the ORM after the instance has been loaded from the DB or otherwise reconstituted
      i.e automatically deserialize Xcom value when loading from DB.



   
   .. method:: __repr__(self)



   
   .. classmethod:: set(cls, key, value, execution_date, task_id, dag_id, session=None)

      Store an XCom value.

      :return: None



   
   .. classmethod:: get_one(cls, execution_date: pendulum.DateTime, key: Optional[str] = None, task_id: Optional[Union[str, Iterable[str]]] = None, dag_id: Optional[Union[str, Iterable[str]]] = None, include_prior_dates: bool = False, session: Session = None)

      Retrieve an XCom value, optionally meeting certain criteria. Returns None
      of there are no results.

      :param execution_date: Execution date for the task
      :type execution_date: pendulum.datetime
      :param key: A key for the XCom. If provided, only XComs with matching
          keys will be returned. To remove the filter, pass key=None.
      :type key: str
      :param task_id: Only XComs from task with matching id will be
          pulled. Can pass None to remove the filter.
      :type task_id: str
      :param dag_id: If provided, only pulls XCom from this DAG.
          If None (default), the DAG of the calling task is used.
      :type dag_id: str
      :param include_prior_dates: If False, only XCom from the current
          execution_date are returned. If True, XCom from previous dates
          are returned as well.
      :type include_prior_dates: bool
      :param session: database session
      :type session: sqlalchemy.orm.session.Session



   
   .. classmethod:: get_many(cls, execution_date: pendulum.DateTime, key: Optional[str] = None, task_ids: Optional[Union[str, Iterable[str]]] = None, dag_ids: Optional[Union[str, Iterable[str]]] = None, include_prior_dates: bool = False, limit: Optional[int] = None, session: Session = None)

      Composes a query to get one or more values from the xcom table.

      :param execution_date: Execution date for the task
      :type execution_date: pendulum.datetime
      :param key: A key for the XCom. If provided, only XComs with matching
          keys will be returned. To remove the filter, pass key=None.
      :type key: str
      :param task_ids: Only XComs from tasks with matching ids will be
          pulled. Can pass None to remove the filter.
      :type task_ids: str or iterable of strings (representing task_ids)
      :param dag_ids: If provided, only pulls XComs from this DAG.
          If None (default), the DAG of the calling task is used.
      :type dag_ids: str
      :param include_prior_dates: If False, only XComs from the current
          execution_date are returned. If True, XComs from previous dates
          are returned as well.
      :type include_prior_dates: bool
      :param limit: If required, limit the number of returned objects.
          XCom objects can be quite big and you might want to limit the
          number of rows.
      :type limit: int
      :param session: database session
      :type session: sqlalchemy.orm.session.Session



   
   .. classmethod:: delete(cls, xcoms, session=None)

      Delete Xcom



   
   .. staticmethod:: serialize_value(value: Any)

      Serialize Xcom value to str or pickled object



   
   .. staticmethod:: deserialize_value(result: 'XCom')

      Deserialize XCom value from str or pickle object



   
   .. method:: orm_deserialize_value(self)

      Deserialize method which is used to reconstruct ORM XCom object.

      This method should be overridden in custom XCom backends to avoid
      unnecessary request or other resource consuming operations when
      creating XCom orm model. This is used when viewing XCom listing
      in the webserver, for example.




.. function:: resolve_xcom_backend()
   Resolves custom XCom class


.. data:: XCom
   

   

