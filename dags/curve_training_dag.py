from http import HTTPStatus
import requests
from airflow.utils.logger import generate_logger
import os
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.utils import timezone
from airflow.models import DAG, DagRun
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import pendulum
from airflow.api.common.experimental.mark_tasks import set_dag_run_final_state
from airflow.utils.curve import do_save_curve_error_tag, get_cas_training_base_url, get_task_params, get_craft_type, \
    generate_bolt_number, get_curve_params, get_result, get_curve, trigger_push_result_to_mq, get_curve_mode, \
    should_trigger_training
from airflow.api.common.experimental.mark_tasks import modify_task_instance
import json

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')
MAX_ACTIVE_TRAINING = os.environ.get('MAX_ACTIVE_TRAINING', 100)

if RUNTIME_ENV == 'prod':
    schedule_interval = None
    write_options = SYNCHRONOUS
else:
    schedule_interval = None
    write_options = ASYNCHRONOUS

_logger = generate_logger(__name__)

DAG_ID = 'curve_training_dag'
TASK_ID = 'curve_training_task'


def onDagFail(context):
    _logger.error("{0} Run Fail".format(context))


def onDagSuccess(context):
    _logger.info("{0} Run Success".format(context))


local_tz = pendulum.timezone("Asia/Shanghai")

desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz),
    'email': ['support@desoutter.cn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': onDagFail,
    'on_success_callback': onDagSuccess,
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}


def update_confirm_data(task_data, verify_error, curve_mode):
    data = task_data.get('task', {})
    data.update({
        "curve_mode": curve_mode,
        "verify_error": verify_error
    })
    return {
        'task': data
    }


def do_trigger_training(task_instance, final_state):
    entity_id = task_instance.entity_id
    url = "{}/cas/invalid-curve".format(get_cas_training_base_url())
    _logger.info('getting result...')
    result = get_result(entity_id)
    _logger.info('getting curve...')
    curve = get_curve(entity_id)
    task_data = get_task_params(task_instance, entity_id)
    curve_mode = get_curve_mode(final_state, task_instance.error_tag)
    task_param = update_confirm_data(task_data,
                                     task_instance.verify_error,
                                     curve_mode
                                     )
    controller_name = result.get('controller_name', None)
    job = result.get('job', None)
    batch_count = result.get('batch_count', None)
    pset = result.get('pset', None)
    bolt_number = generate_bolt_number(controller_name, job, batch_count, pset)
    curve_params = get_curve_params(bolt_number)
    data = {
        'entity_id': entity_id,
        'result': result,
        'curve': curve,
        'craft_type': get_craft_type(bolt_number)
    }
    data.update(task_param)
    data.update(curve_params)
    json_data = {
        'conf': data
    }
    try:
        _logger.info('posting to training server')
        _logger.debug('data:{}'.format(json.dumps(json_data, indent=4)))
        resp = requests.post(headers={'Content-Type': 'application/json'}, url=url, json=json_data, timeout=(3.05, 27))
        _logger.info('training server response')
        if resp.status_code != HTTPStatus.OK:
            raise Exception(resp.content)
    except Exception as e:
        _logger.error(repr(e))
        raise Exception(str(e))


def verify_params(**kwargs):
    dag_run = kwargs.get('dag_run', None)
    params = None
    if isinstance(dag_run, DagRun):
        params = getattr(dag_run, 'conf')
    if isinstance(dag_run, dict):
        params = dag_run.get('conf', None)
    if params is None:
        raise Exception(u'参数conf不存在')
    dag_id = params.get('dag_id')
    task_id = params.get('task_id')
    execution_date = params.get('execution_date')
    final_state = params.get('final_state')
    error_tags = params.get('error_tags', [])
    if not dag_id or not task_id or not execution_date or not final_state:
        raise Exception('empty dag_id, task_id, execution_date or final_state')
    return dag_id, task_id, execution_date, final_state, error_tags


def trigger_training_task(task_instance, **kwargs):
    """触发训练任务"""
    dag_id, task_id, execution_date, final_state, error_tags = verify_params(**kwargs)
    date = timezone.parse(execution_date)
    _logger.info('locating task instance.')
    ana_ti = get_task_instance(dag_id, task_id, execution_date=date)
    _logger.info('task instance located.')

    def modifier(ti):
        ti.entity_id = ana_ti.entity_id
        ti.line_code = ana_ti.line_code

    modify_task_instance(
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.execution_date,
        modifier=modifier
    )
    date = timezone.parse(execution_date)
    task = get_task_instance(dag_id, task_id, date)
    if should_trigger_training(task.result, final_state, task.error_tag, error_tags):
        _logger.info('trigger training...')
        do_trigger_training(ana_ti, final_state)
        _logger.info('training finished, saving error tag')
    else:
        _logger.info('training skipped, saving error tag')
    do_save_curve_error_tag(
        task_id=task_id,
        dag_id=dag_id,
        execution_date=execution_date,
        error_tags=error_tags)
    _logger.info('updating final state...')
    set_dag_run_final_state(
        task_id=task_id,
        dag_id=dag_id,
        execution_date=date,
        final_state=final_state)
    _logger.info('publishing result')
    trigger_push_result_to_mq(
        'final_result',
        final_state,
        ana_ti.entity_id,
        execution_date,
        task_id,
        dag_id,
        verify_error=ana_ti.verify_error,
        curve_mode=ana_ti.error_tag
    )
    _logger.info('all done.')


dag = DAG(
    dag_id=DAG_ID,
    description=u'上汽拧紧曲线训练任务',
    schedule_interval=schedule_interval,
    default_args=desoutter_default_args,
    max_active_runs=MAX_ACTIVE_TRAINING)

store_task = PythonOperator(provide_context=True,
                            task_id=TASK_ID, dag=dag,
                            python_callable=trigger_training_task)
