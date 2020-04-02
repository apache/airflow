#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class Tag(object):
    """Tag class"""

    def __init__(self, content=None):
        self.content = content
        self.attrs = ' '.join(['%s="%s"' % (attr, value)
                               for attr, value in self.attrs])

    def __str__(self):
        return '<%s%s>\n    %s\n</%s>' % (self.name,
                                          ' ' + self.attrs if self.attrs else '',
                                          self.content,
                                          self.name)


class ScriptTag(Tag):
    name = 'script'
    attrs = (('type', 'text/javascript'),)


class AnonymousFunction(object):
    def __init__(self, arguments, content):
        self.arguments = arguments
        self.content = content

    def __str__(self):
        return 'function(%s) { %s }' % (self.arguments, self.content)


class Function(object):

    def __init__(self, name):
        self.name = name
        self._calls = []

    def __str__(self):
        operations = [self.name]
        operations.extend(str(call) for call in self._calls)
        return '%s' % ('.'.join(operations),)

    def __getattr__(self, attr):
        self._calls.append(attr)
        return self

    def __call__(self, *args):
        if not args:
            self._calls[-1] = self._calls[-1] + '()'
        else:
            arguments = ','.join([str(arg) for arg in args])
            self._calls[-1] = self._calls[-1] + '(%s)' % (arguments,)
        return self


class Assignment(object):

    def __init__(self, key, value, scoped=True):
        self.key = key
        self.value = value
        self.scoped = scoped

    def __str__(self):
        return '%s%s = %s;' % ('var ' if self.scoped else '', self.key, self.value)


def indent(func):
    # TODO: Add indents to function str
    return str(func)
