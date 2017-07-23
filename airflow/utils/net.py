from airflow.configuration import conf, AirflowConfigException
import socket


def get_hostname(default=socket.getfqdn):
    """
    A replacement for `socket.getfqdn` that allows configuration to override it's value.

    :param callable|str default: Default if config does not specify. If a callable is given it will be called.
    """
    try:
        # If this is called `get`, why does it not match python semantics?
        hostname = conf.get('core', 'hostname')
    except AirflowConfigException:
        hostname = callable(default) and default() or default
    return hostname
