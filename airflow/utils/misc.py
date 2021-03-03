# -*- coding: utf-8 -*-

from functools import wraps
import cProfile
from typing import List, Any, Optional, Dict


def get_first_valid_data(data: List[Any], key: str) -> Optional[Any]:
    for d in data:
        v = d
        if isinstance(d, Dict) and key:
            v = d.get(key)
        if not v:
            continue
        return v
    return None


class profile(object):
    '''
    Profiler类可以生成单一单数的profile文件

    from airflow.utils.misc import profile
    [...]
    @profile('/temp/prof.profile')
    def mymethod(...)

    gprof2dot -f pstats -o /temp/prof.xdot /temp/prof.profile
    xdot /temp/prof.xdot
    '''

    def __init__(self, fname=None):
        self.fname = fname

    def __call__(self, f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            profile = cProfile.Profile()
            result = profile.runcall(f, *args, **kwargs)
            profile.dump_stats(self.fname or ("%s.cprof" % (f.__name__,)))
            return result

        return wrapper
