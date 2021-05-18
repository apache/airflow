import logging
import re
import sys

from airflow._vendor.connexion import utils
from airflow._vendor.connexion.exceptions import ResolverError

logger = logging.getLogger('connexion.resolver')


class Resolution(object):
    def __init__(self, function, operation_id):
        """
        Represents the result of operation resolution

        :param function: The endpoint function
        :type function: types.FunctionType
        """
        self.function = function
        self.operation_id = operation_id


class Resolver(object):
    def __init__(self, function_resolver=utils.get_function_from_name):
        """
        Standard resolver

        :param function_resolver: Function that resolves functions using an operationId
        :type function_resolver: types.FunctionType
        """
        self.function_resolver = function_resolver

    def resolve(self, operation):
        """
        Default operation resolver

        :type operation: connexion.operations.AbstractOperation
        """
        operation_id = self.resolve_operation_id(operation)
        return Resolution(self.resolve_function_from_operation_id(operation_id), operation_id)

    def resolve_operation_id(self, operation):
        """
        Default operationId resolver

        :type operation: connexion.operations.AbstractOperation
        """
        operation_id = operation.operation_id
        router_controller = operation.router_controller
        if operation.router_controller is None:
            return operation_id
        return '{}.{}'.format(router_controller, operation_id)

    def resolve_function_from_operation_id(self, operation_id):
        """
        Invokes the function_resolver

        :type operation_id: str
        """
        try:
            return self.function_resolver(operation_id)
        except ImportError as e:
            msg = 'Cannot resolve operationId "{}"! Import error was "{}"'.format(operation_id, str(e))
            raise ResolverError(msg, sys.exc_info())
        except (AttributeError, ValueError) as e:
            raise ResolverError(str(e), sys.exc_info())


class RestyResolver(Resolver):
    """
    Resolves endpoint functions using REST semantics (unless overridden by specifying operationId)
    """

    def __init__(self, default_module_name, collection_endpoint_name='search'):
        """
        :param default_module_name: Default module name for operations
        :type default_module_name: str
        """
        Resolver.__init__(self)
        self.default_module_name = default_module_name
        self.collection_endpoint_name = collection_endpoint_name

    def resolve_operation_id(self, operation):
        """
        Resolves the operationId using REST semantics unless explicitly configured in the spec

        :type operation: connexion.operations.AbstractOperation
        """
        if operation.operation_id:
            return Resolver.resolve_operation_id(self, operation)

        return self.resolve_operation_id_using_rest_semantics(operation)

    def resolve_operation_id_using_rest_semantics(self, operation):
        """
        Resolves the operationId using REST semantics

        :type operation: connexion.operations.AbstractOperation
        """
        path_match = re.search(
            r'^/?(?P<resource_name>([\w\-](?<!/))*)(?P<trailing_slash>/*)(?P<extended_path>.*)$', operation.path
        )

        def get_controller_name():
            x_router_controller = operation.router_controller

            name = self.default_module_name
            resource_name = path_match.group('resource_name')

            if x_router_controller:
                name = x_router_controller

            elif resource_name:
                resource_controller_name = resource_name.replace('-', '_')
                name += '.' + resource_controller_name

            return name

        def get_function_name():
            method = operation.method

            is_collection_endpoint = \
                method.lower() == 'get' \
                and path_match.group('resource_name') \
                and not path_match.group('extended_path')

            return self.collection_endpoint_name if is_collection_endpoint else method.lower()

        return '{}.{}'.format(get_controller_name(), get_function_name())


class MethodViewResolver(RestyResolver):
    """
    Resolves endpoint functions based on Flask's MethodView semantics, e.g. ::

            paths:
                /foo_bar:
                    get:
                        # Implied function call: api.FooBarView().get

            class FooBarView(MethodView):
                def get(self):
                    return ...
                def post(self):
                    return ...
    """

    def resolve_operation_id(self, operation):
        """
        Resolves the operationId using REST semantics unless explicitly configured in the spec
        Once resolved with REST semantics the view_name is capitalised and has 'View' added
        to it so it now matches the Class names of the MethodView

        :type operation: connexion.operations.AbstractOperation
        """
        if operation.operation_id:
            # If operation_id is defined then use the higher level API to resolve
            return RestyResolver.resolve_operation_id(self, operation)

        # Use RestyResolver to get operation_id for us (follow their naming conventions/structure)
        operation_id = self.resolve_operation_id_using_rest_semantics(operation)
        module_name, view_base, meth_name = operation_id.rsplit('.', 2)
        view_name = view_base[0].upper() + view_base[1:] + 'View'

        return "{}.{}.{}".format(module_name, view_name, meth_name)

    def resolve_function_from_operation_id(self, operation_id):
        """
        Invokes the function_resolver

        :type operation_id: str
        """

        try:
            module_name, view_name, meth_name = operation_id.rsplit('.', 2)
            if operation_id and not view_name.endswith('View'):
                # If operation_id is not a view then assume it is a standard function
                return self.function_resolver(operation_id)

            mod = __import__(module_name, fromlist=[view_name])
            view_cls = getattr(mod, view_name)
            # Find the class and instantiate it
            view = view_cls()
            func = getattr(view, meth_name)
            # Return the method function of the class
            return func
        except ImportError as e:
            msg = 'Cannot resolve operationId "{}"! Import error was "{}"'.format(
                operation_id, str(e))
            raise ResolverError(msg, sys.exc_info())
        except (AttributeError, ValueError) as e:
            raise ResolverError(str(e), sys.exc_info())
