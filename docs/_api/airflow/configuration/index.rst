:mod:`airflow.configuration`
============================

.. py:module:: airflow.configuration


Module Contents
---------------

.. data:: log
   

   

.. function:: expand_env_var(env_var)
   Expands (potentially nested) env vars by repeatedly applying
   `expandvars` and `expanduser` until interpolation stops having
   any effect.


.. function:: run_command(command)
   Runs command and returns stdout


.. function:: _get_config_value_from_secret_backend(config_key)
   Get Config option values from Secret Backend


.. function:: _read_default_config_file(file_name: str) -> Tuple[str, str]

.. function:: default_config_yaml() -> dict
   Read Airflow configs from YAML file

   :return: Python dictionary containing configs & their info


.. py:class:: AirflowConfigParser(default_config=None, *args, **kwargs)

   Bases: :class:`configparser.ConfigParser`

   Custom Airflow Configparser supporting defaults and deprecated options

   .. attribute:: sensitive_config_values
      

      

   .. attribute:: deprecated_options
      

      

   .. attribute:: deprecated_values
      

      

   
   .. method:: optionxform(self, optionstr: str)



   
   .. method:: _validate(self)



   
   .. method:: _validate_config_dependencies(self)

      Validate that config values aren't invalid given other config values
      or system-level limitations and requirements.



   
   .. method:: _using_old_value(self, old, current_value)



   
   .. method:: _update_env_var(self, section, name, new_value)



   
   .. staticmethod:: _create_future_warning(name, section, current_value, new_value, version)



   
   .. staticmethod:: _env_var_name(section, key)



   
   .. method:: _get_env_var_option(self, section, key)



   
   .. method:: _get_cmd_option(self, section, key)



   
   .. method:: _get_secret_option(self, section, key)

      Get Config option values from Secret Backend



   
   .. method:: get(self, section, key, **kwargs)



   
   .. method:: _get_option_from_default_config(self, section, key, **kwargs)



   
   .. method:: _get_option_from_secrets(self, deprecated_key, deprecated_section, key, section)



   
   .. method:: _get_option_from_commands(self, deprecated_key, deprecated_section, key, section)



   
   .. method:: _get_option_from_config_file(self, deprecated_key, deprecated_section, key, kwargs, section)



   
   .. method:: _get_environment_variables(self, deprecated_key, deprecated_section, key, section)



   
   .. method:: getboolean(self, section, key, **kwargs)



   
   .. method:: getint(self, section, key, **kwargs)



   
   .. method:: getfloat(self, section, key, **kwargs)



   
   .. method:: getimport(self, section, key, **kwargs)

      Reads options, imports the full qualified name, and returns the object.

      In case of failure, it throws an exception a clear message with the key aad the section names

      :return: The object or None, if the option is empty



   
   .. method:: read(self, filenames, encoding=None)



   
   .. method:: read_dict(self, dictionary, source='<dict>')



   
   .. method:: has_option(self, section, option)



   
   .. method:: remove_option(self, section, option, remove_default=True)

      Remove an option if it exists in config from a file or
      default config. If both of config have the same option, this removes
      the option in both configs unless remove_default=False.



   
   .. method:: getsection(self, section: str)

      Returns the section as a dict. Values are converted to int, float, bool
      as required.

      :param section: section from the config
      :rtype: dict



   
   .. method:: write(self, fp, space_around_delimiters=True)



   
   .. method:: as_dict(self, display_source=False, display_sensitive=False, raw=False, include_env=True, include_cmds=True, include_secret=True)

      Returns the current configuration as an OrderedDict of OrderedDicts.

      :param display_source: If False, the option value is returned. If True,
          a tuple of (option_value, source) is returned. Source is either
          'airflow.cfg', 'default', 'env var', or 'cmd'.
      :type display_source: bool
      :param display_sensitive: If True, the values of options set by env
          vars and bash commands will be displayed. If False, those options
          are shown as '< hidden >'
      :type display_sensitive: bool
      :param raw: Should the values be output as interpolated values, or the
          "raw" form that can be fed back in to ConfigParser
      :type raw: bool
      :param include_env: Should the value of configuration from AIRFLOW__
          environment variables be included or not
      :type include_env: bool
      :param include_cmds: Should the result of calling any *_cmd config be
          set (True, default), or should the _cmd options be left as the
          command to run (False)
      :type include_cmds: bool
      :param include_secret: Should the result of calling any *_secret config be
          set (True, default), or should the _secret options be left as the
          path to get the secret from (False)
      :type include_secret: bool
      :rtype: Dict[str, Dict[str, str]]
      :return: Dictionary, where the key is the name of the section and the content is
          the dictionary with the name of the parameter and its value.



   
   .. method:: _include_secrets(self, config_sources, display_sensitive, display_source, raw)



   
   .. method:: _include_commands(self, config_sources, display_sensitive, display_source, raw)



   
   .. method:: _include_envs(self, config_sources, display_sensitive, display_source, raw)



   
   .. staticmethod:: _replace_config_with_display_sources(config_sources, configs, display_source, raw)



   
   .. staticmethod:: _replace_section_config_with_display_sources(config, config_sources, display_source, raw, section, source_name)



   
   .. method:: load_test_config(self)

      Load the unit test configuration.

      Note: this is not reversible.



   
   .. staticmethod:: _warn_deprecate(section, key, deprecated_section, deprecated_name)




.. function:: get_airflow_home()
   Get path to Airflow Home


.. function:: get_airflow_config(airflow_home)
   Get Path to airflow.cfg path


.. data:: AIRFLOW_HOME
   

   

.. data:: AIRFLOW_CONFIG
   

   

.. data:: _TEST_DAGS_FOLDER
   

   

.. data:: TEST_DAGS_FOLDER
   

   

.. data:: _TEST_PLUGINS_FOLDER
   

   

.. data:: TEST_PLUGINS_FOLDER
   

   

.. function:: parameterized_config(template)
   Generates a configuration from the provided template + variables defined in
   current scope

   :param template: a config content templated with {{variables}}


.. function:: get_airflow_test_config(airflow_home)
   Get path to unittests.cfg


.. data:: TEST_CONFIG_FILE
   

   

.. data:: FERNET_KEY
   

   

.. data:: SECRET_KEY
   

   

.. data:: TEMPLATE_START
   :annotation: = # ----------------------- TEMPLATE BEGINS HERE -----------------------

   

.. data:: cfg
   

   

.. data:: cfg
   

   

.. data:: conf
   

   

.. data:: msg
   :annotation: = Specifying both AIRFLOW_HOME environment variable and airflow_home in the config file is deprecated. Please use only the AIRFLOW_HOME environment variable and remove the config file entry.

   

.. data:: WEBSERVER_CONFIG
   

   

.. function:: load_test_config()
   Historical load_test_config


.. function:: get(*args, **kwargs)
   Historical get


.. function:: getboolean(*args, **kwargs)
   Historical getboolean


.. function:: getfloat(*args, **kwargs)
   Historical getfloat


.. function:: getint(*args, **kwargs)
   Historical getint


.. function:: getsection(*args, **kwargs)
   Historical getsection


.. function:: has_option(*args, **kwargs)
   Historical has_option


.. function:: remove_option(*args, **kwargs)
   Historical remove_option


.. function:: as_dict(*args, **kwargs)
   Historical as_dict


.. function:: set(*args, **kwargs)
   Historical set


.. function:: ensure_secrets_loaded() -> List[BaseSecretsBackend]
   Ensure that all secrets backends are loaded.
   If the secrets_backend_list contains only 2 default backends, reload it.


.. function:: get_custom_secret_backend() -> Optional[BaseSecretsBackend]
   Get Secret Backend if defined in airflow.cfg


.. function:: initialize_secrets_backends() -> List[BaseSecretsBackend]
   * import secrets backend classes
   * instantiate them and return them in a list


.. data:: secrets_backend_list
   

   

