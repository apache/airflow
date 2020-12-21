:mod:`airflow.www.views`
========================

.. py:module:: airflow.www.views


Module Contents
---------------

.. data:: PAGE_SIZE
   

   

.. data:: FILTER_TAGS_COOKIE
   :annotation: = tags_filter

   

.. data:: FILTER_STATUS_COOKIE
   :annotation: = dag_status_filter

   

.. function:: get_safe_url(url)
   Given a user-supplied URL, ensure it points to our web server


.. function:: get_date_time_num_runs_dag_runs_form_data(www_request, session, dag)
   Get Execution Data, Base Date & Number of runs from a Request


.. function:: task_group_to_dict(task_group)
   Create a nested dict representation of this TaskGroup and its children used to construct
   the Graph View.


.. function:: dag_edges(dag)
   Create the list of edges needed to construct the Graph View.

   A special case is made if a TaskGroup is immediately upstream/downstream of another
   TaskGroup or task. Two dummy nodes named upstream_join_id and downstream_join_id are
   created for the TaskGroup. Instead of drawing an edge onto every task in the TaskGroup,
   all edges are directed onto the dummy nodes. This is to cut down the number of edges on
   the graph.

   For example: A DAG with TaskGroups group1 and group2:
       group1: task1, task2, task3
       group2: task4, task5, task6

   group2 is downstream of group1:
       group1 >> group2

   Edges to add (This avoids having to create edges between every task in group1 and group2):
       task1 >> downstream_join_id
       task2 >> downstream_join_id
       task3 >> downstream_join_id
       downstream_join_id >> upstream_join_id
       upstream_join_id >> task4
       upstream_join_id >> task5
       upstream_join_id >> task6


.. function:: circles(error)
   Show Circles on screen for any error in the Webserver


.. function:: show_traceback(error)
   Show Traceback for a given error


.. py:class:: AirflowBaseView

   Bases: :class:`flask_appbuilder.BaseView`

   Base View to set Airflow related properties

   .. attribute:: route_base
      :annotation: = 

      

   .. attribute:: extra_args
      

      

   
   .. method:: render_template(self, *args, **kwargs)




.. py:class:: Airflow

   Bases: :class:`airflow.www.views.AirflowBaseView`

   Main Airflow application.

   
   .. method:: health(self)

      An endpoint helping check the health status of the Airflow instance,
      including metadatabase and scheduler.



   
   .. method:: index(self)

      Home view.



   
   .. method:: dag_stats(self, session=None)

      Dag statistics.



   
   .. method:: task_stats(self, session=None)

      Task Statistics



   
   .. method:: last_dagruns(self, session=None)

      Last DAG runs



   
   .. method:: code(self, session=None)

      Dag Code.



   
   .. method:: dag_details(self, session=None)

      Get Dag details.



   
   .. method:: rendered_templates(self)

      Get rendered Dag.



   
   .. method:: rendered_k8s(self)

      Get rendered k8s yaml.



   
   .. method:: get_logs_with_metadata(self, session=None)

      Retrieve logs including metadata.



   
   .. method:: log(self, session=None)

      Retrieve log.



   
   .. method:: redirect_to_external_log(self, session=None)

      Redirects to external log.



   
   .. method:: task(self)

      Retrieve task.



   
   .. method:: xcom(self, session=None)

      Retrieve XCOM.



   
   .. method:: run(self)

      Runs Task Instance.



   
   .. method:: delete(self)

      Deletes DAG.



   
   .. method:: trigger(self, session=None)

      Triggers DAG Run.



   
   .. method:: _clear_dag_tis(self, dag, start_date, end_date, origin, recursive=False, confirmed=False, only_failed=False)



   
   .. method:: clear(self)

      Clears the Dag.



   
   .. method:: dagrun_clear(self)

      Clears the DagRun



   
   .. method:: blocked(self, session=None)

      Mark Dag Blocked.



   
   .. method:: _mark_dagrun_state_as_failed(self, dag_id, execution_date, confirmed, origin)



   
   .. method:: _mark_dagrun_state_as_success(self, dag_id, execution_date, confirmed, origin)



   
   .. method:: dagrun_failed(self)

      Mark DagRun failed.



   
   .. method:: dagrun_success(self)

      Mark DagRun success



   
   .. method:: _mark_task_instance_state(self, dag_id, task_id, origin, execution_date, confirmed, upstream, downstream, future, past, state)



   
   .. method:: failed(self)

      Mark task as failed.



   
   .. method:: success(self)

      Mark task as success.



   
   .. method:: tree(self)

      Get Dag as tree.



   
   .. method:: graph(self, session=None)

      Get DAG as Graph.



   
   .. method:: duration(self, session=None)

      Get Dag as duration graph.



   
   .. method:: tries(self, session=None)

      Shows all tries.



   
   .. method:: landing_times(self, session=None)

      Shows landing times.



   
   .. method:: paused(self)

      Toggle paused.



   
   .. method:: refresh(self, session=None)

      Refresh DAG.



   
   .. method:: refresh_all(self)

      Refresh everything



   
   .. method:: gantt(self, session=None)

      Show GANTT chart.



   
   .. method:: extra_links(self)

      A restful endpoint that returns external links for a given Operator

      It queries the operator that sent the request for the links it wishes
      to provide for a given external link name.

      API: GET
      Args: dag_id: The id of the dag containing the task in question
            task_id: The id of the task in question
            execution_date: The date of execution of the task
            link_name: The name of the link reference to find the actual URL for

      Returns:
          200: {url: <url of link>, error: None} - returned when there was no problem
              finding the URL
          404: {url: None, error: <error message>} - returned when the operator does
              not return a URL



   
   .. method:: task_instances(self)

      Shows task instances.




.. py:class:: ConfigurationView

   Bases: :class:`airflow.www.views.AirflowBaseView`

   View to show Airflow Configurations

   .. attribute:: default_view
      :annotation: = conf

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: base_permissions
      

      

   
   .. method:: conf(self)

      Shows configuration.




.. py:class:: RedocView

   Bases: :class:`airflow.www.views.AirflowBaseView`

   Redoc Open API documentation

   .. attribute:: default_view
      :annotation: = redoc

      

   
   .. method:: redoc(self)

      Redoc API documentation.




.. py:class:: DagFilter

   Bases: :class:`flask_appbuilder.models.sqla.filters.BaseFilter`

   Filter using DagIDs

   
   .. method:: apply(self, query, func)




.. py:class:: AirflowModelView

   Bases: :class:`flask_appbuilder.ModelView`

   Airflow Mode View.

   .. attribute:: list_widget
      

      

   .. attribute:: page_size
      

      

   .. attribute:: CustomSQLAInterface
      

      


.. py:class:: SlaMissModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show SlaMiss table

   .. attribute:: route_base
      :annotation: = /slamiss

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: list_columns
      :annotation: = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']

      

   .. attribute:: add_columns
      :annotation: = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']

      

   .. attribute:: edit_columns
      :annotation: = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']

      

   .. attribute:: search_columns
      :annotation: = ['dag_id', 'task_id', 'email_sent', 'timestamp', 'execution_date']

      

   .. attribute:: base_order
      :annotation: = ['execution_date', 'desc']

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   .. attribute:: formatters_columns
      

      


.. py:class:: XComModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from XCom table

   .. attribute:: route_base
      :annotation: = /xcom

      

   .. attribute:: list_title
      :annotation: = List XComs

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: search_columns
      :annotation: = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']

      

   .. attribute:: list_columns
      :annotation: = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']

      

   .. attribute:: base_order
      :annotation: = ['execution_date', 'desc']

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   .. attribute:: formatters_columns
      

      

   
   .. method:: action_muldelete(self, items)

      Multiple delete action.



   
   .. method:: pre_add(self, item)

      Pre add hook.



   
   .. method:: pre_update(self, item)

      Pre update hook.




.. py:class:: ConnectionModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from Connections table

   .. attribute:: route_base
      :annotation: = /connection

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: extra_fields
      :annotation: = ['extra__jdbc__drv_path', 'extra__jdbc__drv_clsname', 'extra__google_cloud_platform__project', 'extra__google_cloud_platform__key_path', 'extra__google_cloud_platform__keyfile_dict', 'extra__google_cloud_platform__scope', 'extra__google_cloud_platform__num_retries', 'extra__grpc__auth_type', 'extra__grpc__credential_pem_file', 'extra__grpc__scopes', 'extra__yandexcloud__service_account_json', 'extra__yandexcloud__service_account_json_path', 'extra__yandexcloud__oauth', 'extra__yandexcloud__public_ssh_key', 'extra__yandexcloud__folder_id', 'extra__kubernetes__in_cluster', 'extra__kubernetes__kube_config', 'extra__kubernetes__namespace']

      

   .. attribute:: list_columns
      :annotation: = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted', 'is_extra_encrypted']

      

   .. attribute:: add_template
      :annotation: = airflow/conn_create.html

      

   .. attribute:: edit_template
      :annotation: = airflow/conn_edit.html

      

   .. attribute:: base_order
      :annotation: = ['conn_id', 'asc']

      

   
   .. method:: action_muldelete(self, items)

      Multiple delete.



   
   .. method:: process_form(self, form, is_created)

      Process form data.



   
   .. method:: prefill_form(self, form, pk)

      Prefill the form.




.. py:class:: PluginView

   Bases: :class:`airflow.www.views.AirflowBaseView`

   View to show Airflow Plugins

   .. attribute:: default_view
      :annotation: = list

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: plugins_attributes_to_dump
      :annotation: = ['hooks', 'executors', 'macros', 'admin_views', 'flask_blueprints', 'menu_links', 'appbuilder_views', 'appbuilder_menu_items', 'global_operator_extra_links', 'operator_extra_links', 'source']

      

   
   .. method:: list(self)

      List loaded plugins.




.. py:class:: PoolModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from Pool table

   .. attribute:: route_base
      :annotation: = /pool

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: list_columns
      :annotation: = ['pool', 'slots', 'running_slots', 'queued_slots']

      

   .. attribute:: add_columns
      :annotation: = ['pool', 'slots', 'description']

      

   .. attribute:: edit_columns
      :annotation: = ['pool', 'slots', 'description']

      

   .. attribute:: base_order
      :annotation: = ['pool', 'asc']

      

   .. attribute:: formatters_columns
      

      

   .. attribute:: validators_columns
      

      

   
   .. method:: action_muldelete(self, items)

      Multiple delete.



   
   .. method:: pool_link(self)

      Pool link rendering.



   
   .. method:: frunning_slots(self)

      Running slots rendering.



   
   .. method:: fqueued_slots(self)

      Queued slots rendering.




.. py:class:: VariableModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from Variable table

   .. attribute:: route_base
      :annotation: = /variable

      

   .. attribute:: list_template
      :annotation: = airflow/variable_list.html

      

   .. attribute:: edit_template
      :annotation: = airflow/variable_edit.html

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: list_columns
      :annotation: = ['key', 'val', 'is_encrypted']

      

   .. attribute:: add_columns
      :annotation: = ['key', 'val']

      

   .. attribute:: edit_columns
      :annotation: = ['key', 'val']

      

   .. attribute:: search_columns
      :annotation: = ['key', 'val']

      

   .. attribute:: base_order
      :annotation: = ['key', 'asc']

      

   .. attribute:: formatters_columns
      

      

   .. attribute:: validators_columns
      

      

   
   .. method:: hidden_field_formatter(self)

      Formats hidden fields



   
   .. method:: prefill_form(self, form, request_id)



   
   .. method:: action_muldelete(self, items)

      Multiple delete.



   
   .. method:: action_varexport(self, items)

      Export variables.



   
   .. method:: varimport(self)

      Import variables




.. py:class:: JobModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from Job table

   .. attribute:: route_base
      :annotation: = /job

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: list_columns
      :annotation: = ['id', 'dag_id', 'state', 'job_type', 'start_date', 'end_date', 'latest_heartbeat', 'executor_class', 'hostname', 'unixname']

      

   .. attribute:: search_columns
      :annotation: = ['id', 'dag_id', 'state', 'job_type', 'start_date', 'end_date', 'latest_heartbeat', 'executor_class', 'hostname', 'unixname']

      

   .. attribute:: base_order
      :annotation: = ['start_date', 'desc']

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   .. attribute:: formatters_columns
      

      


.. py:class:: DagRunModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from DagRun table

   .. attribute:: route_base
      :annotation: = /dagrun

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: add_columns
      :annotation: = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger', 'conf']

      

   .. attribute:: list_columns
      :annotation: = ['state', 'dag_id', 'execution_date', 'run_id', 'run_type', 'external_trigger', 'conf']

      

   .. attribute:: search_columns
      :annotation: = ['state', 'dag_id', 'execution_date', 'run_id', 'run_type', 'external_trigger', 'conf']

      

   .. attribute:: base_order
      :annotation: = ['execution_date', 'desc']

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   .. attribute:: formatters_columns
      

      

   
   .. method:: action_muldelete(self, items, session=None)

      Multiple delete.



   
   .. method:: action_set_running(self, drs, session=None)

      Set state to running.



   
   .. method:: action_set_failed(self, drs, session=None)

      Set state to failed.



   
   .. method:: action_set_success(self, drs, session=None)

      Set state to success.



   
   .. method:: action_clear(self, drs, session=None)

      Clears the state.




.. py:class:: LogModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from Log table

   .. attribute:: route_base
      :annotation: = /log

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: list_columns
      :annotation: = ['id', 'dttm', 'dag_id', 'task_id', 'event', 'execution_date', 'owner', 'extra']

      

   .. attribute:: search_columns
      :annotation: = ['dag_id', 'task_id', 'event', 'execution_date', 'owner', 'extra']

      

   .. attribute:: base_order
      :annotation: = ['dttm', 'desc']

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   .. attribute:: formatters_columns
      

      


.. py:class:: TaskRescheduleModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from Task Reschedule table

   .. attribute:: route_base
      :annotation: = /taskreschedule

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: list_columns
      :annotation: = ['id', 'dag_id', 'task_id', 'execution_date', 'try_number', 'start_date', 'end_date', 'duration', 'reschedule_date']

      

   .. attribute:: search_columns
      :annotation: = ['dag_id', 'task_id', 'execution_date', 'start_date', 'end_date', 'reschedule_date']

      

   .. attribute:: base_order
      :annotation: = ['id', 'desc']

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   .. attribute:: formatters_columns
      

      

   
   .. method:: duration_f(self)

      Duration calculation.




.. py:class:: TaskInstanceModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from TaskInstance table

   .. attribute:: route_base
      :annotation: = /taskinstance

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: page_size
      

      

   .. attribute:: list_columns
      :annotation: = ['state', 'dag_id', 'task_id', 'execution_date', 'operator', 'start_date', 'end_date', 'duration', 'job_id', 'hostname', 'unixname', 'priority_weight', 'queue', 'queued_dttm', 'try_number', 'pool', 'log_url']

      

   .. attribute:: order_columns
      

      

   .. attribute:: search_columns
      :annotation: = ['state', 'dag_id', 'task_id', 'execution_date', 'hostname', 'queue', 'pool', 'operator', 'start_date', 'end_date']

      

   .. attribute:: base_order
      :annotation: = ['job_id', 'asc']

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   .. attribute:: formatters_columns
      

      

   
   .. method:: log_url_formatter(self)

      Formats log URL.



   
   .. method:: duration_f(self)

      Formats duration.



   
   .. method:: action_clear(self, task_instances, session=None)

      Clears the action.



   
   .. method:: set_task_instance_state(self, tis, target_state, session=None)

      Set task instance state.



   
   .. method:: action_set_running(self, tis)

      Set state to 'running'



   
   .. method:: action_set_failed(self, tis)

      Set state to 'failed'



   
   .. method:: action_set_success(self, tis)

      Set state to 'success'



   
   .. method:: action_set_retry(self, tis)

      Set state to 'up_for_retry'




.. py:class:: DagModelView

   Bases: :class:`airflow.www.views.AirflowModelView`

   View to show records from DAG table

   .. attribute:: route_base
      :annotation: = /dagmodel

      

   .. attribute:: datamodel
      

      

   .. attribute:: class_permission_name
      

      

   .. attribute:: method_permission_name
      

      

   .. attribute:: base_permissions
      

      

   .. attribute:: list_columns
      :annotation: = ['dag_id', 'is_paused', 'last_scheduler_run', 'last_expired', 'scheduler_lock', 'fileloc', 'owners']

      

   .. attribute:: formatters_columns
      

      

   .. attribute:: base_filters
      :annotation: = [None]

      

   
   .. method:: get_query(self)

      Default filters for model



   
   .. method:: get_count_query(self)

      Default filters for model



   
   .. method:: autocomplete(self, session=None)

      Autocomplete.




