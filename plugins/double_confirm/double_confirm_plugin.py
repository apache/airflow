import datetime
from flask import jsonify, request
from flask_appbuilder import BaseView, expose, has_access
from flask_login import current_user
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import TIMEZONE
from plugins.utils.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
from airflow.exceptions import AirflowException
from plugins.utils.utils import trigger_training_dag, get_result
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.app import csrf

_log = LoggingMixin().log


class DoubleConfirmView(BaseView):
    route_base = ''

    @expose('/double-confirm/<string:entity_id>', methods=['POST'])
    @has_access
    def double_confirm_task(self, entity_id):
        try:
            msg = CUSTOM_LOG_FORMAT.format(datetime.datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                           current_user, getattr(current_user, 'last_name', ''),
                                           CUSTOM_EVENT_NAME_MAP['DOUBLE_CONFIRM'], CUSTOM_PAGE_NAME_MAP['CURVE'],
                                           '曲线二次确认')
            logging.info(msg)
            params = request.get_json(force=True)  # success failed
            final_state = params.get('final_state', None)
            error_tags = params.get('error_tags', [])
            entity_id = entity_id.replace('@', '/')
            result = get_result(entity_id)
            if not result.get('result'):
                raise AirflowException(u"分析结果还没有生成，请等待分析结果生成后再进行二次确认")
            if not final_state or final_state not in ['OK', 'NOK']:
                raise AirflowException("二次确认参数未定义或数值不正确!")
            trigger_training_dag(entity_id, final_state, error_tags)
            return jsonify({'response': 'ok'})
        except AirflowException as err:
            _log.info(err)
            response = jsonify(error="{}".format(err))
            response.status_code = err.status_code
            return response

    @csrf.exempt
    @expose('/dags/<string:dag_id>/tasks/<string:task_id>/<string:execution_date>/error_tag', methods=['POST'])
    @has_access
    def save_curve_error_tag(self, dag_id, task_id, execution_date):
        return self._save_curve_error_tag(dag_id, task_id, execution_date)

    @csrf.exempt
    @expose('/error_tag/dags/<string:dag_id>/tasks/<string:task_id>/<string:execution_date>', methods=['POST'])
    @has_access
    def save_curve_error_tag_w_csrf(self, dag_id, task_id, execution_date):
        return self._save_curve_error_tag(dag_id, task_id, execution_date)

    def _save_curve_error_tag(self, dag_id, task_id, execution_date):
        try:
            params = request.get_json(force=True)  # success failed
            error_tags = params.get('error_tags', [])
            # fixme
            # do_save_curve_error_tag(dag_id, task_id, execution_date, error_tags)
            return jsonify(response='ok')
        except Exception as e:
            _log.info(repr(e))
            response = jsonify({'error': repr(e)})
            response.status_code = 400
            return response


double_confirm_view = DoubleConfirmView()
double_confirm_view_package = {"view": double_confirm_view}


# Defining the plugin class
class DoubleConfirmPlugin(AirflowPlugin):
    name = "double_confirm_plugin"
    appbuilder_views = [double_confirm_view_package]
