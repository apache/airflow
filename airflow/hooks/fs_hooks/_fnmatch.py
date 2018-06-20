"""Filename matching with shell patterns.

Modified version of the stblib implementation that only matches '*.csv'
in the current directory and supports recursive matching with '**/*.csv'.
"""

import os
import posixpath
import re
import functools


__all__ = ["filter", "fnmatch", "fnmatchcase", "translate"]


def fnmatch(name, pat, sep=os.sep):
    """Test whether FILENAME matches PATTERN.

    Patterns are Unix shell style:

    *       matches everything except seperator
    ?       matches any single character
    [seq]   matches any character in seq
    [!seq]  matches any char not in seq

    An initial period in FILENAME is not special.
    Both FILENAME and PATTERN are first case-normalized
    if the operating system requires it.
    If you don't want this, use fnmatchcase(FILENAME, PATTERN).
    """
    name = os.path.normcase(name)
    pat = os.path.normcase(pat)
    return fnmatchcase(name, pat, sep=sep)


@functools.lru_cache(maxsize=256, typed=True)
def _compile_pattern(pat, sep):
    if isinstance(pat, bytes):
        pat_str = str(pat, 'ISO-8859-1')
        res_str = translate(pat_str, sep=sep)
        res = bytes(res_str, 'ISO-8859-1')
    else:
        res = translate(pat, sep=sep)
    return re.compile(res).match


def filter(names, pat, sep=os.sep):
    """Return the subset of the list NAMES that match PAT."""
    result = []
    pat = os.path.normcase(pat)
    match = _compile_pattern(pat, sep=sep)
    if os.path is posixpath:
        # normcase on posix is NOP. Optimize it away from the loop.
        for name in names:
            if match(name):
                result.append(name)
    else:
        for name in names:
            if match(os.path.normcase(name)):
                result.append(name)
    return result


def fnmatchcase(name, pat, sep=os.sep):
    """Test whether FILENAME matches PATTERN, including case.

    This is a version of fnmatch() which doesn't case-normalize
    its arguments.
    """
    match = _compile_pattern(pat, sep=sep)
    return match(name) is not None


def translate(pat, sep=os.sep):
    """Translate a shell PATTERN to a regular expression.

    There is no way to quote meta-characters.
    """

    star_repl = '[^{}]*'.format(re.escape(sep))
    dbl_star_repl = '.*(?={})'.format(re.escape(sep))

    i, n = 0, len(pat)
    res = ''
    while i < n:
        c = pat[i]
        i = i + 1
        if c == '*':
            if i < n and pat[i] == '*':
                res = res + dbl_star_repl
                i = i + 1
            else:
                res = res + star_repl
        elif c == '?':
            res = res + '.'
        elif c == '[':
            j = i
            if j < n and pat[j] == '!':
                j = j + 1
            if j < n and pat[j] == ']':
                j = j + 1
            while j < n and pat[j] != ']':
                j = j + 1
            if j >= n:
                res = res + '\\['
            else:
                stuff = pat[i:j].replace('\\', '\\\\')
                i = j + 1
                if stuff[0] == '!':
                    stuff = '^' + stuff[1:]
                elif stuff[0] == '^':
                    stuff = '\\' + stuff
                res = '%s[%s]' % (res, stuff)
        else:
            res = res + re.escape(c)

    return r'(?s:%s)\Z' % res
