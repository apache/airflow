import functools
import importlib

import yaml


def boolean(s):
    '''
    Convert JSON/Swagger boolean value to Python, raise ValueError otherwise

    >>> boolean('true')
    True

    >>> boolean('false')
    False
    '''
    if isinstance(s, bool):
        return s
    elif not hasattr(s, 'lower'):
        raise ValueError('Invalid boolean value')
    elif s.lower() == 'true':
        return True
    elif s.lower() == 'false':
        return False
    else:
        raise ValueError('Invalid boolean value')


# https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#data-types
TYPE_MAP = {'integer': int,
            'number': float,
            'string': str,
            'boolean': boolean,
            'array': list,
            'object': dict}  # map of swagger types to python types


def make_type(value, _type):
    type_func = TYPE_MAP[_type]  # convert value to right type
    return type_func(value)


def deep_merge(a, b):
    """ merges b into a
        in case of conflict the value from b is used
    """
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                deep_merge(a[key], b[key])
            elif a[key] == b[key]:
                pass
            else:
                # b overwrites a
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a


def deep_getattr(obj, attr):
    """
    Recurses through an attribute chain to get the ultimate value.
    """

    attrs = attr.split('.')

    return functools.reduce(getattr, attrs, obj)


def deep_get(obj, keys):
    """
    Recurses through a nested object get a leaf value.

    There are cases where the use of inheritance or polymorphism-- the use of allOf or
    oneOf keywords-- will cause the obj to be a list. In this case the keys will
    contain one or more strings containing integers.

    :type obj: list or dict
    :type keys: list of strings
    """
    if not keys:
        return obj

    if isinstance(obj, list):
        return deep_get(obj[int(keys[0])], keys[1:])
    else:
        return deep_get(obj[keys[0]], keys[1:])


def get_function_from_name(function_name):
    """
    Tries to get function by fully qualified name (e.g. "mymodule.myobj.myfunc")

    :type function_name: str
    """
    if function_name is None:
        raise ValueError("Empty function name")

    if '.' in function_name:
        module_name, attr_path = function_name.rsplit('.', 1)
    else:
        module_name = ''
        attr_path = function_name

    module = None
    last_import_error = None

    while not module:
        try:
            module = importlib.import_module(module_name)
        except ImportError as import_error:
            last_import_error = import_error
            if '.' in module_name:
                module_name, attr_path1 = module_name.rsplit('.', 1)
                attr_path = '{0}.{1}'.format(attr_path1, attr_path)
            else:
                raise
    try:
        function = deep_getattr(module, attr_path)
    except AttributeError:
        if last_import_error:
            raise last_import_error
        else:
            raise
    return function


def is_json_mimetype(mimetype):
    """
    :type mimetype: str
    :rtype: bool
    """
    maintype, subtype = mimetype.split('/')  # type: str, str
    return maintype == 'application' and (subtype == 'json' or subtype.endswith('+json'))


def all_json(mimetypes):
    """
    Returns True if all mimetypes are serialized with json

    :type mimetypes: list
    :rtype: bool

    >>> all_json(['application/json'])
    True
    >>> all_json(['application/x.custom+json'])
    True
    >>> all_json([])
    True
    >>> all_json(['application/xml'])
    False
    >>> all_json(['text/json'])
    False
    >>> all_json(['application/json', 'other/type'])
    False
    >>> all_json(['application/json', 'application/x.custom+json'])
    True
    """
    return all(is_json_mimetype(mimetype) for mimetype in mimetypes)


def is_nullable(param_def):
    return (
        param_def.get('schema', param_def).get('nullable', False) or
        param_def.get('x-nullable', False)  # swagger2
    )


def is_null(value):
    if hasattr(value, 'strip') and value.strip() in ['null', 'None']:
        return True

    if value is None:
        return True

    return False


def has_coroutine(function, api=None):
    """
    Checks if function is a coroutine.
    If ``function`` is a decorator (has a ``__wrapped__`` attribute)
    this function will also look at the wrapped function.
    """
    import asyncio

    def iscorofunc(func):
        iscorofunc = asyncio.iscoroutinefunction(func)
        while not iscorofunc and hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            iscorofunc = asyncio.iscoroutinefunction(func)
        return iscorofunc

    if api is None:
        return iscorofunc(function)

    else:
        return any(
            iscorofunc(func) for func in (
                function, api.get_request, api.get_response
            )
        )


def yamldumper(openapi):
    """
    Returns a nicely-formatted yaml spec.
    :param openapi: a spec dictionary.
    :return: a nicely-formatted, serialized yaml spec.
    """
    def should_use_block(value):
        char_list = (
          u"\u000a"  # line feed
          u"\u000d"  # carriage return
          u"\u001c"  # file separator
          u"\u001d"  # group separator
          u"\u001e"  # record separator
          u"\u0085"  # next line
          u"\u2028"  # line separator
          u"\u2029"  # paragraph separator
        )
        for c in char_list:
            if c in value:
                return True
        return False

    def my_represent_scalar(self, tag, value, style=None):
        if should_use_block(value):
            style = '|'
        else:
            style = self.default_style

        node = yaml.representer.ScalarNode(tag, value, style=style)
        if self.alias_key is not None:
            self.represented_objects[self.alias_key] = node
        return node

    class NoAnchorDumper(yaml.dumper.SafeDumper):
        """A yaml Dumper that does not replace duplicate entries
           with yaml anchors.
        """

        def ignore_aliases(self, *args):
            return True

    # Dump long lines as "|".
    yaml.representer.SafeRepresenter.represent_scalar = my_represent_scalar

    return yaml.dump(openapi, allow_unicode=True, Dumper=NoAnchorDumper)
