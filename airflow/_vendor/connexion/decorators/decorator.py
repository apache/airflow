import functools
import logging

from ..utils import has_coroutine

logger = logging.getLogger('connexion.decorators.decorator')


class BaseDecorator(object):

    def __call__(self, function):
        """
        :type function: types.FunctionType
        :rtype: types.FunctionType
        """
        return function

    def __repr__(self):  # pragma: no cover
        """
        :rtype: str
        """
        return '<BaseDecorator>'


class RequestResponseDecorator(BaseDecorator):
    """Manages the lifecycle of the request internally in Connexion.
    Filter the ConnexionRequest instance to return the corresponding
    framework specific object.
    """

    def __init__(self, api, mimetype):
        self.api = api
        self.mimetype = mimetype

    def __call__(self, function):
        """
        :type function: types.FunctionType
        :rtype: types.FunctionType
        """
        if has_coroutine(function, self.api):
            from .coroutine_wrappers import get_request_life_cycle_wrapper
            wrapper = get_request_life_cycle_wrapper(function, self.api, self.mimetype)

        else:  # pragma: 3 no cover
            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                request = self.api.get_request(*args, **kwargs)
                response = function(request)
                return self.api.get_response(response, self.mimetype, request)

        return wrapper
