from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.plugins_manager import AirflowPlugin
import logging
from flask_appbuilder import BaseView, expose
from airflow.models import Variable
from plugins.utils.utils import get_curve, get_result
from plugins.models.error_tag import ErrorTag
from plugins.utils.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
from datetime import datetime
import os
from airflow.settings import TIMEZONE
from flask_login import current_user
from airflow.security import permissions
_logger = logging.getLogger(__name__)


class CurveView(BaseView):
    route_base = ''

    base_permissions = [permissions.ACTION_CAN_READ]

    class_permission_name = permissions.RESOURCE_CURVE

    method_permission_name = {
        'view_curve_page': 'read'
    }

    base_permissions = [
        permissions.ACTION_CAN_READ
    ]

    @expose('/view_curve/<string:entity_id>')
    def view_curve_page(self, entity_id: str):
        entity_id = entity_id.replace('@', '/')

        _has_access = self.appbuilder.sm.has_access

        try:
            result = get_result(entity_id)
        except Exception as e:
            _logger.error(e)
            result = {}
        try:
            curve = get_curve(entity_id)
        except Exception as e:
            _logger.error(e)
            curve = {}

        analysis_error_message_mapping = Variable.get('analysis_error_message_mapping', deserialize_json=True,
                                                      default_var={})

        verify_error_map = Variable.get('verify_error_map', deserialize_json=True,
                                        default_var={})

        result_error_message_mapping = Variable.get('result_error_message_mapping', deserialize_json=True,
                                                    default_var={})

        controller_name = result.get('controller_name', '').split('@')[0] if result.get('controller_name') else ''
        from plugins.models.tightening_controller import TighteningController
        controller = TighteningController.find_controller(controller_name)
        error_tags = ErrorTag.get_all()
        ENV_CURVE_GRAPH_SHOW_RANGE = os.environ.get('CURVE_GRAPH_SHOW_RANGE')
        show_range = (ENV_CURVE_GRAPH_SHOW_RANGE is True) or (ENV_CURVE_GRAPH_SHOW_RANGE == 'True')
        can_verify = _has_access(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_RESULT)
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['CURVE'], '查看单条曲线')
        _logger.info(msg)

        if result.get('device_type') == 'servo_press':
            cur_key_map = {
                'cur_w': '位移',
                'cur_m': '压力',
                'cur_t': '时间',
            }
            display_keys = Variable.get('servo_press_view_curve_page_keys', deserialize_json=True,
                                        default_var={})
            result_keys_translation_mapping = Variable.get('servo_press_result_keys_translation_mapping',
                                                           deserialize_json=True,
                                                           default_var={})
        else:
            cur_key_map = {
                'cur_w': '角度',
                'cur_m': '扭矩',
                'cur_t': '时间',
                'cur_s': '转速'
            }
            display_keys = Variable.get('view_curve_page_keys', deserialize_json=True,
                                        default_var={})
            result_keys_translation_mapping = Variable.get('result_keys_translation_mapping',
                                                           deserialize_json=True,
                                                           default_var={})

        return self.render_template('curve.html', result=result,
                                    curve=curve, analysisErrorMessageMapping=analysis_error_message_mapping,
                                    resultErrorMessageMapping=result_error_message_mapping,
                                    resultKeysTranslationMapping=result_keys_translation_mapping,
                                    verify_error_map=verify_error_map,
                                    can_verify=can_verify,
                                    controller=controller,
                                    errorTags=error_tags,
                                    show_range=show_range,
                                    display_keys=display_keys,
                                    cur_key_map=cur_key_map
                                    )


curve_view = CurveView()
curve_view_package = {"view": curve_view}


class CurveViewPlugin(AirflowPlugin):
    name = "curve_view_plugin"
    appbuilder_views = [curve_view_package]
