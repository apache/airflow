"""
Module dedicated to networking operations.
"""


import socket
from contextlib import closing


def find_free_port() -> int:
    """
    Finds the first free local port.

    :return: first free port found.
    :rtype: int
    """

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as a_socket:
        a_socket.bind(("127.0.0.1", 0))
        a_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return a_socket.getsockname()[1]


def check_if_port_is_being_listened_on(port: int) -> bool:
    """
    Checks if provided local port is being listened on.

    :param port: port to check.
    :type port: int

    :return: True if port is being listened on and False otherwise.
    :rtype: bool
    """

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as a_socket:
        location = ("127.0.0.1", port)
        result_of_check = a_socket.connect_ex(location)
        return result_of_check == 0
