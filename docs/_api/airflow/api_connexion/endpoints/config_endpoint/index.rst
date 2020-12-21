:mod:`airflow.api_connexion.endpoints.config_endpoint`
======================================================

.. py:module:: airflow.api_connexion.endpoints.config_endpoint


Module Contents
---------------

.. data:: LINE_SEP
   :annotation: = 


   

.. function:: _conf_dict_to_config(conf_dict: dict) -> Config
   Convert config dict to a Config object


.. function:: _option_to_text(config_option: ConfigOption) -> str
   Convert a single config option to text


.. function:: _section_to_text(config_section: ConfigSection) -> str
   Convert a single config section to text


.. function:: _config_to_text(config: Config) -> str
   Convert the entire config to text


.. function:: _config_to_json(config: Config) -> str
   Convert a Config object to a JSON formatted string


.. function:: get_config() -> Response
   Get current configuration.


