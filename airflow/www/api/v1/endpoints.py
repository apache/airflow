from airflow import configuration
from airflow.www.views import dagbag
from airflow.www import utils as wwwutils
from airflow.configuration import AirflowConfigException

from importlib import import_module

import logging

LOG = logging.getLogger(__name__)

from flask import (
    url_for, Markup, Blueprint, redirect, jsonify
)

apiv1 = Blueprint('apiv1', __name__)


@apiv1.route('/sync_dags')
#apiv1.route('/<key>/sync_dags')
def sync_dags():
    logging.info("Requesting sync of dag folder")
    try:
        synchronizer_module = configuration.get('core', 'dag_synchronizer')
        synchronizer = import_module(synchronizer_module).get_instance()

        synchronizer.sync()
    except AirflowConfigException:
        logging.info("dag_synchronizer not configured in airflow configuration. Please specify"
                     "[core]/dag_synchronizer")
    except ImportError:
        logging.warn("Cannot load {} as synchronizer for dag folder".format(synchronizer_module))
        logging.exception(ImportError)

    LOG.error("Requesting sync_dag_folder from workers")
    from airflow.executors import DEFAULT_EXECUTOR as executor
    executor.sync_dag_folder()

    dagbag.refresh()

    return wwwutils.json_response({'status': 'EXECUTED'})


