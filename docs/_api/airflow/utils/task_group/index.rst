:mod:`airflow.utils.task_group`
===============================

.. py:module:: airflow.utils.task_group

.. autoapi-nested-parse::

   A TaskGroup is a collection of closely related tasks on the same DAG that should be grouped
   together when the DAG is displayed graphically.



Module Contents
---------------

.. py:class:: TaskGroup(group_id: Optional[str], prefix_group_id: bool = True, parent_group: Optional['TaskGroup'] = None, dag: Optional['DAG'] = None, tooltip: str = '', ui_color: str = 'CornflowerBlue', ui_fgcolor: str = '#000')

   Bases: :class:`airflow.models.taskmixin.TaskMixin`

   A collection of tasks. When set_downstream() or set_upstream() are called on the
   TaskGroup, it is applied across all tasks within the group if necessary.

   :param group_id: a unique, meaningful id for the TaskGroup. group_id must not conflict
       with group_id of TaskGroup or task_id of tasks in the DAG. Root TaskGroup has group_id
       set to None.
   :type group_id: str
   :param prefix_group_id: If set to True, child task_id and group_id will be prefixed with
       this TaskGroup's group_id. If set to False, child task_id and group_id are not prefixed.
       Default is True.
   :type prerfix_group_id: bool
   :param parent_group: The parent TaskGroup of this TaskGroup. parent_group is set to None
       for the root TaskGroup.
   :type parent_group: TaskGroup
   :param dag: The DAG that this TaskGroup belongs to.
   :type dag: airflow.models.DAG
   :param tooltip: The tooltip of the TaskGroup node when displayed in the UI
   :type tooltip: str
   :param ui_color: The fill color of the TaskGroup node when displayed in the UI
   :type ui_color: str
   :param ui_fgcolor: The label color of the TaskGroup node when displayed in the UI
   :type ui_fgcolor: str

   .. attribute:: is_root
      

      Returns True if this TaskGroup is the root TaskGroup. Otherwise False


   .. attribute:: group_id
      

      group_id of this TaskGroup.


   .. attribute:: label
      

      group_id excluding parent's group_id used as the node label in UI.


   .. attribute:: roots
      

      Required by TaskMixin


   .. attribute:: leaves
      

      Required by TaskMixin


   .. attribute:: upstream_join_id
      

      If this TaskGroup has immediate upstream TaskGroups or tasks, a dummy node called
      upstream_join_id will be created in Graph View to join the outgoing edges from this
      TaskGroup to reduce the total number of edges needed to be displayed.


   .. attribute:: downstream_join_id
      

      If this TaskGroup has immediate downstream TaskGroups or tasks, a dummy node called
      downstream_join_id will be created in Graph View to join the outgoing edges from this
      TaskGroup to reduce the total number of edges needed to be displayed.


   
   .. classmethod:: create_root(cls, dag: 'DAG')

      Create a root TaskGroup with no group_id or parent.



   
   .. method:: __iter__(self)



   
   .. method:: add(self, task: Union['BaseOperator', 'TaskGroup'])

      Add a task to this TaskGroup.



   
   .. method:: update_relative(self, other: 'TaskMixin', upstream=True)

      Overrides TaskMixin.update_relative.

      Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids
      accordingly so that we can reduce the number of edges when displaying Graph View.



   
   .. method:: _set_relative(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]], upstream: bool = False)

      Call set_upstream/set_downstream for all root/leaf tasks within this TaskGroup.
      Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids.



   
   .. method:: set_downstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]])

      Set a TaskGroup/task/list of task downstream of this TaskGroup.



   
   .. method:: set_upstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]])

      Set a TaskGroup/task/list of task upstream of this TaskGroup.



   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, _type, _value, _tb)



   
   .. method:: has_task(self, task: 'BaseOperator')

      Returns True if this TaskGroup or its children TaskGroups contains the given task.



   
   .. method:: get_roots(self)

      Returns a generator of tasks that are root tasks, i.e. those with no upstream
      dependencies within the TaskGroup.



   
   .. method:: get_leaves(self)

      Returns a generator of tasks that are leaf tasks, i.e. those with no downstream
      dependencies within the TaskGroup



   
   .. method:: child_id(self, label)

      Prefix label with group_id if prefix_group_id is True. Otherwise return the label
      as-is.



   
   .. method:: get_task_group_dict(self)

      Returns a flat dictionary of group_id: TaskGroup



   
   .. method:: get_child_by_label(self, label: str)

      Get a child task/TaskGroup by its label (i.e. task_id/group_id without the group_id prefix)




.. py:class:: TaskGroupContext

   TaskGroup context is used to keep the current TaskGroup when TaskGroup is used as ContextManager.

   .. attribute:: _context_managed_task_group
      :annotation: :Optional[TaskGroup]

      

   .. attribute:: _previous_context_managed_task_groups
      :annotation: :List[TaskGroup] = []

      

   
   .. classmethod:: push_context_managed_task_group(cls, task_group: TaskGroup)

      Push a TaskGroup into the list of managed TaskGroups.



   
   .. classmethod:: pop_context_managed_task_group(cls)

      Pops the last TaskGroup from the list of manged TaskGroups and update the current TaskGroup.



   
   .. classmethod:: get_current_task_group(cls, dag: Optional['DAG'])

      Get the current TaskGroup.




