from .abstract import AbstractOperation  # noqa
from .openapi import OpenAPIOperation  # noqa
from .secure import SecureOperation  # noqa
from .swagger2 import Swagger2Operation  # noqa


def make_operation(spec, *args, **kwargs):
    return spec.operation_cls.from_spec(spec, *args, **kwargs)
