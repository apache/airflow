import socket

import pendulum

from airflow.configuration import conf


def init_jinja_globals(app):
    server_timezone = conf.get('core', 'default_timezone')
    if server_timezone == "system":
        server_timezone = pendulum.local_timezone().name
    elif server_timezone == "utc":
        server_timezone = "UTC"

    default_ui_timezone = conf.get('webserver', 'default_ui_timezone')
    if default_ui_timezone == "system":
        default_ui_timezone = pendulum.local_timezone().name
    elif default_ui_timezone == "utc":
        default_ui_timezone = "UTC"
    if not default_ui_timezone:
        default_ui_timezone = server_timezone

    @app.context_processor
    def jinja_globals():  # pylint: disable=unused-variable

        globals = {
            'server_timezone': server_timezone,
            'default_ui_timezone': default_ui_timezone,
            'hostname': socket.getfqdn() if conf.getboolean(
                'webserver', 'EXPOSE_HOSTNAME', fallback=True) else 'redact',
            'navbar_color': conf.get(
                'webserver', 'NAVBAR_COLOR'),
            'log_fetch_delay_sec': conf.getint(
                'webserver', 'log_fetch_delay_sec', fallback=2),
            'log_auto_tailing_offset': conf.getint(
                'webserver', 'log_auto_tailing_offset', fallback=30),
            'log_animation_speed': conf.getint(
                'webserver', 'log_animation_speed', fallback=1000)
        }

        if 'analytics_tool' in conf.getsection('webserver'):
            globals.update({
                'analytics_tool': conf.get('webserver', 'ANALYTICS_TOOL'),
                'analytics_id': conf.get('webserver', 'ANALYTICS_ID')
            })

        return globals
