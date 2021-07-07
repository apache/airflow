from TrainingServer.sdk import sdk, structs
import os
from unittest import TestCase
from test.test_data import curve_param, curve, cluster_data

CURVE_LIB_PATH = os.environ.get(
    'CURVE_LIB_PATH',
    '../TrainingServer/lib/libcurve_analysis_platform.so')

bolt_number = 'W1-021R_1_1'
craft_type = 1


def callEncode(mode):
    sdk.doCACurveEncode(bolt_number, craft_type, mode, curve_param, curve)


class TestSDK(TestCase):
    @classmethod
    def setUpClass(cls):
        sdk.create_logger()
        sdk.initCurveLib(CURVE_LIB_PATH)

    def test_get_platform_version(self):
        assert sdk.get_platform_version() is not None

    def test_get_algorithm_version(self):
        assert sdk.get_algorithm_version() is not None

    def test_encode_ok(self):
        callEncode(0)

    def test_encode_nok(self):
        callEncode(1)

    def test_curve_template(self):
        c_template_cluster = structs.templateData2CStructure(cluster_data)
        sdk.doCALoadCurveTemplate(
            bolt_number,
            craft_type,
            0,
            curve_param,
            c_template_cluster)
        assert sdk.doCAGetCurveTemplate(bolt_number, 0)

# if __name__ == '__main__':
#     create_logger()
#     InitCurveLib('./cmake-build-debug/libcurve_uat_lib.so')
#     doCAFunction('ca_attach', 111)
#     doCAFunction('ca_deattach')
#     p = C.create_string_buffer(256)
#     doCAFunction('ca_get_version', 1, p, 256)
#     _logger.info(repr(p.value))
#     cc = newCurveParams()
#     bolt_number = uuid.uuid4()
#     print(bolt_number)
#     CurveLib.ca_load_curve_template(C.create_string_buffer(str(bolt_number).encode('utf-8')), 1, cc, 22, 22)
#     CurveLib.ca_release_curve_template(C.create_string_buffer(str(bolt_number).encode('utf-8')))
#     length = C.c_long(0)
#     CurveLib.ca_get_template_len(C.create_string_buffer(str(bolt_number).encode('utf-8')), C.pointer(length))
#     print(length.value)
#
#     craft_type = CRAFT_TYPE(0)
#     CurveLib.ca_get_craft_type(C.create_string_buffer(str(bolt_number).encode('utf-8')), C.pointer(craft_type))
#     print(craft_type.value)
#
#     params = CURVE_PARAM()
#     doCAFunction('ca_get_curve_params', C.create_string_buffer(str(bolt_number).encode('utf-8')), C.pointer(params))
#     print(params.threshold)
#
#     data = doCACurveEncode(str(bolt_number), CRAFT_TYPE(1), 0, params, [1.1, 1.2, 3.4])
#     print(data)
#     ret = doCACurveVerify(str(bolt_number), [1.1, 1.2, 3.4], np.array([[12.0, 99.9], [22.2, 33.5], [22.2, 99.5]]))
#     print(ret)
