from airflow import configuration

from flask import (
    url_for, Markup, Blueprint, redirect,
)

apiv1 = Blueprint('apiv1', __name__)


@apiv1.route('/sync_dags')
#api.route('/<key>/sync_dags')
def sync_dags():
    from airflow.executors import DEFAULT_EXECUTOR as executor
    executor.start()
    command = configuration.get("core", "sync_command").split()
    executor.queue_command(command)
    executor.heartbeat()


