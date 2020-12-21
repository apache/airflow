:mod:`airflow.www.security`
===========================

.. py:module:: airflow.www.security


Module Contents
---------------

.. data:: EXISTING_ROLES
   

   

.. py:class:: AirflowSecurityManager(appbuilder)

   Bases: :class:`flask_appbuilder.security.sqla.manager.SecurityManager`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Custom security manager, which introduces an permission model adapted to Airflow

   .. attribute:: VIEWER_PERMISSIONS
      :annotation: = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]

      

   .. attribute:: USER_PERMISSIONS
      :annotation: = [None, None, None, None, None, None, None, None]

      

   .. attribute:: OP_PERMISSIONS
      :annotation: = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]

      

   .. attribute:: ADMIN_PERMISSIONS
      :annotation: = [None, None]

      

   .. attribute:: DAG_VMS
      

      

   .. attribute:: READ_DAG_PERMS
      

      

   .. attribute:: DAG_PERMS
      

      

   .. attribute:: ROLE_CONFIGS
      

      

   
   .. method:: init_role(self, role_name, perms)

      Initialize the role with the permissions and related view-menus.

      :param role_name:
      :param perms:
      :return:



   
   .. method:: add_permissions(self, role, perms)

      Adds resource permissions to a given role.



   
   .. method:: delete_role(self, role_name)

      Delete the given Role

      :param role_name: the name of a role in the ab_role table



   
   .. staticmethod:: get_user_roles(user=None)

      Get all the roles associated with the user.

      :param user: the ab_user in FAB model.
      :return: a list of roles associated with the user.



   
   .. method:: get_all_permissions_views(self)

      Returns a set of tuples with the perm name and view menu name



   
   .. method:: get_readable_dags(self, user)

      Gets the DAGs readable by authenticated user.



   
   .. method:: get_editable_dags(self, user)

      Gets the DAGs editable by authenticated user.



   
   .. method:: get_readable_dag_ids(self, user)

      Gets the DAG IDs readable by authenticated user.



   
   .. method:: get_editable_dag_ids(self, user)

      Gets the DAG IDs editable by authenticated user.



   
   .. method:: get_accessible_dag_ids(self, user)

      Gets the DAG IDs editable or readable by authenticated user.



   
   .. method:: get_accessible_dags(self, user_actions, user, session=None)

      Generic function to get readable or writable DAGs for authenticated user.



   
   .. method:: can_access_some_dags(self, action: str, dag_id: Optional[int] = None)

      Checks if user has read or write access to some dags.



   
   .. method:: can_read_dag(self, dag_id, user=None)

      Determines whether a user has DAG read access.



   
   .. method:: can_edit_dag(self, dag_id, user=None)

      Determines whether a user has DAG edit access.



   
   .. method:: prefixed_dag_id(self, dag_id)

      Returns the permission name for a DAG id.



   
   .. method:: is_dag_resource(self, resource_name)

      Determines if a permission belongs to a DAG or all DAGs.



   
   .. method:: has_access(self, permission, resource, user=None)

      Verify whether a given user could perform certain permission
      (e.g can_read, can_write) on the given resource.

      :param permission: permission on resource (e.g can_read, can_edit).
      :type permission: str
      :param resource: name of view-menu or resource.
      :type resource: str
      :param user: user name
      :type user: str
      :return: a bool whether user could perform certain permission on the resource.
      :rtype bool



   
   .. method:: _get_and_cache_perms(self)

      Cache permissions-views



   
   .. method:: _has_role(self, role_name_or_list)

      Whether the user has this role name



   
   .. method:: _has_perm(self, permission_name, view_menu_name)

      Whether the user has this perm



   
   .. method:: has_all_dags_access(self)

      Has all the dag access in any of the 3 cases:
      1. Role needs to be in (Admin, Viewer, User, Op).
      2. Has can_read permission on dags view.
      3. Has can_edit permission on dags view.



   
   .. method:: clean_perms(self)

      FAB leaves faulty permissions that need to be cleaned up



   
   .. method:: _merge_perm(self, permission_name, view_menu_name)

      Add the new (permission, view_menu) to assoc_permissionview_role if it doesn't exist.
      It will add the related entry to ab_permission
      and ab_view_menu two meta tables as well.

      :param permission_name: Name of the permission.
      :type permission_name: str
      :param view_menu_name: Name of the view-menu
      :type view_menu_name: str
      :return:



   
   .. method:: create_custom_dag_permission_view(self, session=None)

      Workflow:
      1. Fetch all the existing (permissions, view-menu) from Airflow DB.
      2. Fetch all the existing dag models that are either active or paused.
      3. Create both read and write permission view-menus relation for every dags from step 2
      4. Find out all the dag specific roles(excluded pubic, admin, viewer, op, user)
      5. Get all the permission-vm owned by the user role.
      6. Grant all the user role's permission-vm except the all-dag view-menus to the dag roles.
      7. Commit the updated permission-vm-role into db

      :return: None.



   
   .. method:: update_admin_perm_view(self)

      Admin should have all the permission-views, except the dag views.
      because Admin already has Dags permission.
      Add the missing ones to the table for admin.

      :return: None.



   
   .. method:: sync_roles(self)

      1. Init the default role(Admin, Viewer, User, Op, public)
         with related permissions.
      2. Init the custom role(dag-user) with related permissions.

      :return: None.



   
   .. method:: sync_resource_permissions(self, perms=None)

      Populates resource-based permissions.



   
   .. method:: sync_perm_for_dag(self, dag_id, access_control=None)

      Sync permissions for given dag id. The dag id surely exists in our dag bag
      as only / refresh button or cli.sync_perm will call this function

      :param dag_id: the ID of the DAG whose permissions should be updated
      :type dag_id: str
      :param access_control: a dict where each key is a rolename and
          each value is a set() of permission names (e.g.,
          {'can_read'}
      :type access_control: dict
      :return:



   
   .. method:: _sync_dag_view_permissions(self, dag_id, access_control)

      Set the access policy on the given DAG's ViewModel.

      :param dag_id: the ID of the DAG whose permissions should be updated
      :type dag_id: str
      :param access_control: a dict where each key is a rolename and
          each value is a set() of permission names (e.g.,
          {'can_read'}
      :type access_control: dict



   
   .. method:: create_perm_vm_for_all_dag(self)

      Create perm-vm if not exist and insert into FAB security model for all-dags.



   
   .. method:: check_authorization(self, perms: Optional[Sequence[Tuple[str, str]]] = None, dag_id: Optional[int] = None)

      Checks that the logged in user has the specified permissions.




