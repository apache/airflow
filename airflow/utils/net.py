from airflow.configuration import conf
import socket

_sentinel = object()


def get_hostname(default=socket.getfqdn):
    """
    A replacement for `socket.getfqdn` that allows configuration to override it's value.

    :param callable|str default: Default if config does not specify. If a callable is given it will be called.
    """
    hostname = conf.get('core', 'HOSTNAME', default)
    if callable(hostname):
        hostname = hostname()
    return hostname
