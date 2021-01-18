# -*- coding: utf-8 -*-
import os
import time

APP_NAME = os.getenv('ENV_APP_NAME', 'QCOS')
FACTORY_CODE = os.getenv('FACTORY_CODE', 'DEFAULT_FACTORY_CODE')
LOCAL_TIME = time.asctime(time.localtime(time.time()))

CUSTOM_LOG_FORMAT = u'%s@@@%s@@@%s@@@{}@@@{}@@@{}@@@{}@@@{}' % (
    LOCAL_TIME, APP_NAME, FACTORY_CODE)
# {USER_NAME}@@@{USER_DOMAIN_NAME}@@@{EVENT_NAME}@@@{PAGE_NAME}@@@{EXTRA_INFO}

CUSTOM_EVENT_NAME_MAP = {'DOUBLE_CONFIRM': '10001',
                         'LOGIN': '10002',
                         'LOGOUT': '10003',
                         'VIEW': '10004',
                         'ADD': '10005',
                         'DELETE': '10006',
                         'EDIT': '10007', }

CUSTOM_PAGE_NAME_MAP = {'LOGIN': '50001',
                        'LOGOUT': '50002',
                        'CURVE': '50003',
                        'CURVES': '50004',
                        'CURVE_TEMPLATE': '50005',
                        'ERROR_TAG': '50006',
                        'TIGHTENING_CONTROLLER': '50007',
                        'TIGHTENING_CURVE_TEMPLATE': '50008', }
