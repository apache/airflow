import os

factory_code_map = {
    'nd': ['nd', '7200', 'ND'],
    'lg': ['lg', '2200', 'LG'],
}


def get_factory_code():
    code = os.environ.get('FACTORY_CODE', '')
    for key, values in factory_code_map.items():
        if code in values:
            return key
    return code
