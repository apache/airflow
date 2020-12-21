:mod:`airflow.providers.singularity.operators.singularity`
==========================================================

.. py:module:: airflow.providers.singularity.operators.singularity


Module Contents
---------------

.. py:class:: SingularityOperator(*, image: str, command: Union[str, ast.AST], start_command: Optional[Union[str, List[str]]] = None, environment: Optional[Dict[str, Any]] = None, pull_folder: Optional[str] = None, working_dir: Optional[str] = None, force_pull: Optional[bool] = False, volumes: Optional[List[str]] = None, options: Optional[List[str]] = None, auto_remove: Optional[bool] = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Execute a command inside a Singularity container

   Singularity has more seamless connection to the host than Docker, so
   no special binds are needed to ensure binding content in the user $HOME
   and temporary directories. If the user needs custom binds, this can
   be done with --volumes

   :param image: Singularity image or URI from which to create the container.
   :type image: str
   :param auto_remove: Delete the container when the process exits
                       The default is False.
   :type auto_remove: bool
   :param command: Command to be run in the container. (templated)
   :type command: str or list
   :param start_command: start command to pass to the container instance
   :type start_command: string or list
   :param environment: Environment variables to set in the container. (templated)
   :type environment: dict
   :param working_dir: Set a working directory for the instance.
   :type working_dir: str
   :param force_pull: Pull the image on every run. Default is False.
   :type force_pull: bool
   :param volumes: List of volumes to mount into the container, e.g.
       ``['/host/path:/container/path', '/host/path2:/container/path2']``.
   :param options: other flags (list) to provide to the instance start
   :type options: list
   :param working_dir: Working directory to
       set on the container (equivalent to the -w switch the docker client)
   :type working_dir: str

   .. attribute:: template_fields
      :annotation: = ['command', 'environment']

      

   .. attribute:: template_ext
      :annotation: = ['.sh', '.bash']

      

   
   .. method:: execute(self, context)



   
   .. method:: _get_command(self)



   
   .. method:: on_kill(self)




