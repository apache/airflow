:mod:`airflow.providers.segment.operators.segment_track_event`
==============================================================

.. py:module:: airflow.providers.segment.operators.segment_track_event


Module Contents
---------------

.. py:class:: SegmentTrackEventOperator(*, user_id: str, event: str, properties: Optional[dict] = None, segment_conn_id: str = 'segment_default', segment_debug_mode: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Send Track Event to Segment for a specified user_id and event

   :param user_id: The ID for this user in your database. (templated)
   :type user_id: str
   :param event: The name of the event you're tracking. (templated)
   :type event: str
   :param properties: A dictionary of properties for the event. (templated)
   :type properties: dict
   :param segment_conn_id: The connection ID to use when connecting to Segment.
   :type segment_conn_id: str
   :param segment_debug_mode: Determines whether Segment should run in debug mode.
       Defaults to False
   :type segment_debug_mode: bool

   .. attribute:: template_fields
      :annotation: = ['user_id', 'event', 'properties']

      

   .. attribute:: ui_color
      :annotation: = #ffd700

      

   
   .. method:: execute(self, context: Dict)




