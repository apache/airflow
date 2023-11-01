Per default raw HTML code in parameter descriptions is disabled with 2.8.0.

To ensure that no maliceus javascript can be injected with Trigger UI form by DAG authors
a new parameter ``webserver.trigger_form_param_html_trust_level`` was added with default value of ``None``.
If you trust your DAG authors code and want to allow using raw HTML in DAG params and restore the previous
behavior you must set the configuration value to ``FullTrust``.
