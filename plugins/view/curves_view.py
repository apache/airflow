from flask_appbuilder.urltools import get_filter_args, get_page_args
import http
import zipfile
import json
from flask_login import current_user
from airflow.settings import TIMEZONE
from datetime import datetime
from typing import List
from flask import Response
from flask import send_file
from flask_appbuilder import expose
from flask_babel import lazy_gettext
from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.configuration import conf
from airflow.exceptions import AirflowNotFoundException
from plugins.models.error_tag import ErrorTag
from airflow.www import utils as wwwutils
from plugins.utils.utils import get_curve_entity_ids, get_curve, get_result, get_results
from plugins.utils.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
import logging
import os
import pandas as pd
from pathlib import Path
from airflow.configuration import AIRFLOW_HOME
from airflow.plugins_manager import AirflowPlugin
from plugins.common import AirflowModelView
from flask import jsonify, request
from airflow.exceptions import AirflowException
from airflow.security import permissions

_logger = logging.getLogger(__name__)

PAGE_SIZE = conf.getint('webserver', 'page_size')


class CurvesView(AirflowModelView):
    list_template = "curves.html"
    CustomSQLAInterface = wwwutils.CustomSQLAInterface
    route_base = '/curves'
    from plugins.models.result import ResultModel
    datamodel = CustomSQLAInterface(ResultModel)
    search_columns = ['execution_date', 'car_code', 'error_tag', 'measure_result', 'result', 'final_state']
    label_columns = {
        'error_tag': lazy_gettext('Error Tags'),
        'execution_date': lazy_gettext('Execution Date'), 'car_code': lazy_gettext('Car Code'),
        'measure_result': lazy_gettext('Measure Result'), 'result': lazy_gettext('Result'),
        'final_state': lazy_gettext('Final State')
    }

    download_static_folder = os.path.join(AIRFLOW_HOME, 'downloads/contents')
    class_permission_name = permissions.RESOURCE_CURVES

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    method_permission_name = {
        'view_curves_analysis': 'read',
        'download': 'read',
        'view_curves': 'read',
        'get_curves_by_entity_id': 'read',
        'get_curves': 'read'
    }

    def __init__(self, *args, **kwargs):
        ret = super(CurvesView, self).__init__(**kwargs)
        os.makedirs(self.download_static_folder, exist_ok=True)

    def do_render(self, track_no=None, bolt_no=None, controller=None, craft_type=None):
        view_name = 'curves'
        curves = request.args.get('curves')
        curves_list = curves.replace('@', '/').split(',') if curves is not None else []
        pages = get_page_args()
        page = pages.get(view_name, 0)
        get_filter_args(self._filters)
        if bolt_no:
            self._filters.add_filter(column_name='bolt_number', filter_class=self.datamodel.FilterEqual, value=bolt_no)
        if craft_type:
            self._filters.add_filter(column_name='craft_type', filter_class=self.datamodel.FilterEqual,
                                     value=int(craft_type))
        if track_no:
            self._filters.add_filter(column_name='car_code', filter_class=self.datamodel.FilterEqual, value=track_no)
        if controller:
            self._filters.add_filter(column_name='controller_name', filter_class=self.datamodel.FilterContains,
                                     value=controller)

        joined_filters = self._filters.get_joined_filters(self._base_filters)
        order_column, order_direction = "execution_date", "desc"
        page_size = PAGE_SIZE
        count, lst = self.datamodel.query(
            joined_filters,
            order_column,
            order_direction,
            page=page,
            page_size=page_size,
        )

        error_tag_vals = ErrorTag.get_all_dict() or {}
        device_type = None
        for t in lst:
            ret = []
            if device_type is None and t.device_type is not None:
                device_type = t.device_type
            try:
                error_tags = json.loads(t.error_tag or '[]')
                if not error_tags:
                    t.view_error_tags = u'无异常标签'
                    continue
                for tag in error_tags:
                    v = error_tag_vals.get(str(tag), '')
                    if not v:
                        continue
                    ret.append(v)
            except Exception as e:
                t.view_error_tags = ','.join(ret)
            t.view_error_tags = ','.join(ret)

        selected_results = {}
        results = list(get_results(curves_list))

        for result in results:
            selected_results[result.get('entity_id')] = {
                'carCode': result.get('car_code'),
                'value': result.get('entity_id'),
                'date': str(result.get('execution_date'))
            }
        widgets = self._list()

        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['CURVES'], '查看曲线对比页面')
        logging.info(msg)

        if device_type == 'servo_press':
            cur_key_map = {
                'cur_w': '位移',
                'cur_m': '压力',
                'cur_t': '时间',
            }
        else:
            cur_key_map = {
                'cur_w': '角度',
                'cur_m': '扭矩',
                'cur_t': '时间',
                'cur_s': '转速'
            }

        return self.render_template('curves.html', results=lst, page=page, page_size=page_size, count=count,
                                    modelview_name=view_name,
                                    selected_curves=curves_list,
                                    selected_results=selected_results,
                                    cur_key_map=cur_key_map,
                                    widgets=widgets)

    @expose('/analysis')
    def view_curves_analysis(self):
        track_no = request.args.get('track_no', default=None)
        bolt_no = request.args.get('bolt_no', default=None)
        controller = request.args.get('controller', default=None)
        analysis_type = request.args.get('analysis_type', default=None)
        ret = None
        if not analysis_type:
            raise AirflowNotFoundException
        if analysis_type == 'track_no' and track_no:
            ret = self.do_render(track_no=track_no)
        elif analysis_type == 'bolt_no' and bolt_no:
            ret = self.do_render(bolt_no=bolt_no)
        elif analysis_type == 'controller' and controller:
            ret = self.do_render(controller=controller)
        if not ret:
            raise AirflowNotFoundException
        return ret

    def clean_download_static_files(self):
        fds = ['*.json', '*.csv']
        for fd in fds:
            for f in Path(self.download_static_folder).glob(fd):
                try:
                    f.unlink()
                except OSError as e:
                    _logger.error(f"Error: {f} : {e}")

    def do_download_contents(self, entities: List[str]) -> List[str]:
        files = []
        base_path = self.download_static_folder
        result_table = pd.DataFrame()
        for entity_id in entities:
            try:
                result = get_result(entity_id)
                result["step_results"] = json.dumps(result['step_results'])
                tb = pd.DataFrame(result, index=[0])
                result_table = pd.concat([result_table, tb], ignore_index=True)
            except Exception as e:
                _logger.error(e)
            try:
                curve = get_curve(entity_id)
                f = f'{entity_id}.csv'.replace('/', '@')
                f = os.path.join(base_path, f)
                dd = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in curve.items()]))
                dd.to_csv(f, index=False, header=True)
                files.append(f)
            except Exception as e:
                _logger.error(e)
        try:
            rf = os.path.join(base_path, "results.csv")
            result_table.to_csv(rf, index=False, header=True)
            files.append(rf)
        except Exception as e:
            _logger.error(e)
        return files

    def generate_download_zip_file(self, files: List[str]):
        try:
            fn = f'{self.download_static_folder}/curves.zip'
            with zipfile.ZipFile(fn, 'w') as f:
                for file in files:
                    if not os.path.exists(file):
                        continue
                    f.write(file, arcname=os.path.basename(file), compress_type=zipfile.ZIP_DEFLATED)
            return True
        except Exception as e:
            _logger.error(e)
            return False

    @expose('/download/<string:entity_ids>')
    def download(self, entity_ids: str):
        if not entity_ids or entity_ids == 'None':
            return Response(status=http.HTTPStatus.OK)

        fn = f'{self.download_static_folder}/curves.zip'
        chk_file = Path(fn)

        if chk_file.is_file():
            chk_file.unlink()
        entity_ids = entity_ids.replace('@', '/')
        entities = entity_ids.split(',')
        ll = len(entities)
        if ll > 500:
            return Response(status=http.HTTPStatus.BAD_REQUEST, response=f'请求的曲线数量过大,最大只能500条，当前为{ll}')
        files = self.do_download_contents(entities)
        if not files:
            return Response(status=http.HTTPStatus.BAD_REQUEST, response=f'未生成数据')
        ret = self.generate_download_zip_file(files)
        if not ret:
            return Response(status=http.HTTPStatus.BAD_REQUEST, response=f'未生成压缩包数据')
        return send_file(fn, mimetype='application/zip', attachment_filename='curves.zip',
                         as_attachment=True)

    @expose('/<string:bolt_no>/<string:craft_type>')
    def view_curves(self, bolt_no, craft_type):
        ret = self.do_render(bolt_no=bolt_no, craft_type=craft_type)
        return ret

    @expose(
        '/curves',
        methods=['GET'])
    def get_curves_by_entity_id(self):
        try:
            curves = []

            vals = request.args.get('entity_ids')
            entity_ids = str(vals).split(",")
            if entity_ids is None:
                return jsonify(curves)

            for entity_id in entity_ids:
                try:
                    curve = get_curve(entity_id)
                    if curve is not None:
                        curves.append({
                            'entity_id': entity_id,
                            'curve': curve
                        })
                except Exception as e:
                    _logger.debug(e)
                    curves.append({
                        'entity_id': entity_id,
                        'curve': []
                    })

            return jsonify(curves=curves)
        except AirflowException as e:
            _logger.error("get_curves_by_entity_id", e)
            response = jsonify(error="{}".format(repr(e)))
            response.status_code = e.status_code
            return response

    @expose('/curve-entities', methods=['GET'])
    def get_curves(self):
        try:
            craft_type = request.args.get('craft_type')
            bolt_number = request.args.get('bolt_number')
            entity_ids = get_curve_entity_ids(bolt_number, craft_type)
            return jsonify(entity_ids)
        except AirflowException as e:
            _logger.error(e)
            response = jsonify(error="{}".format(e))
            response.status_code = e.status_code
            return response


curves_view = CurvesView()
curves_view_package = {"view": curves_view}


class CurvesViewPlugin(AirflowPlugin):
    name = "curves_view_plugin"
    appbuilder_views = [curves_view_package]
