import functools
import random
import re
import string

import flask
import werkzeug.wrappers

PATH_PARAMETER = re.compile(r'\{([^}]*)\}')

# map Swagger type to flask path converter
# see http://flask.pocoo.org/docs/0.10/api/#url-route-registrations
PATH_PARAMETER_CONVERTERS = {
    'integer': 'int',
    'number': 'float',
    'path': 'path'
}


def flaskify_endpoint(identifier, randomize=None):
    """
    Converts the provided identifier in a valid flask endpoint name

    :type identifier: str
    :param randomize: If specified, add this many random characters (upper case
        and digits) to the endpoint name, separated by a pipe character.
    :type randomize: int | None
    :rtype: str
    """
    result = identifier.replace('.', '_')
    if randomize is None:
        return result

    chars = string.ascii_uppercase + string.digits
    return "{result}|{random_string}".format(
        result=result,
        random_string=''.join(random.SystemRandom().choice(chars) for _ in range(randomize)))


def convert_path_parameter(match, types):
    name = match.group(1)
    swagger_type = types.get(name)
    converter = PATH_PARAMETER_CONVERTERS.get(swagger_type)
    return '<{0}{1}{2}>'.format(converter or '',
                                ':' if converter else '',
                                name.replace('-', '_'))


def flaskify_path(swagger_path, types=None):
    """
    Convert swagger path templates to flask path templates

    :type swagger_path: str
    :type types: dict
    :rtype: str

    >>> flaskify_path('/foo-bar/{my-param}')
    '/foo-bar/<my_param>'

    >>> flaskify_path('/foo/{someint}', {'someint': 'int'})
    '/foo/<int:someint>'
    """
    if types is None:
        types = {}
    convert_match = functools.partial(convert_path_parameter, types=types)
    return PATH_PARAMETER.sub(convert_match, swagger_path)


def is_flask_response(obj):
    """
    Verifies if obj is a default Flask response instance.

    :type obj: object
    :rtype bool

    >>> is_flask_response(redirect('http://example.com/'))
    True
    >>> is_flask_response(flask.Response())
    True
    """
    return isinstance(obj, flask.Response) or isinstance(obj, werkzeug.wrappers.Response)
