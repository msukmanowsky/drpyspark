from __future__ import print_function
import functools
import inspect
import logging
import re
import pprint
import sys

try:
    import pyspark
except ImportError:
    pyspark = None

from .version import VERSION
log = logging.getLogger(__package__)


def print_output(f, num_elements, out_file, truncate_dataframes):
    @functools.wraps(f)
    def _debug_pyspark_call(*args, **kwargs):
        log.debug('%s called', f.__name__)
        stack = inspect.stack()
        caller = stack[1]
        caller_package = inspect.getmodule(caller[0]).__package__

        result = f(*args, **kwargs)
        if caller_package in ('pyspark', __package__):
            log.debug('%s internal call to %s, returning to avoid infinite '
                      'recursion', caller_package, f.__name__)
            return result
        if not isinstance(result, (pyspark.RDD, pyspark.sql.DataFrame)):
            log.debug('%s returned non RDD/DataFrame value, returning',
                      f.__name__)
            return result

        if isinstance(result, pyspark.RDD):
            sample = pprint.pformat(result.take(num_elements))
        elif isinstance(result, pyspark.sql.DataFrame):
            sample = result._jdf.showString(num_elements, truncate_dataframes)
        file, line_no, code = stack[-1][1], stack[-1][2], ''.join(stack[-1][4]).strip()
        print('{}:{}: {}'.format(file, line_no, code), file=out_file)
        print(sample, file=out_file)

        return result

    return _debug_pyspark_call


def enable_debug_output(num_elements=5, file=sys.stdout, truncate_dataframes=True):
    if pyspark is None:
        print('pyspark not found in PYTHONPATH, did you run via spark-submit?',
              file=sys.stderr)
        sys.exit(1)

    classes_to_patch = (pyspark.SparkContext, pyspark.sql.SQLContext,
                        pyspark.sql.HiveContext, pyspark.RDD,
                        pyspark.sql.DataFrame)
    for klass in classes_to_patch:
        members = inspect.getmembers(klass)
        # get all public methods not starting with _ or save
        methods = [(name, member) for (name, member) in members
                   if not name.startswith('_') and inspect.ismethod(member)]
        for name, method in methods:
            setattr(klass, name, print_output(method, num_elements, file, truncate_dataframes))
            log.debug('Patched %s.%s.', klass.__name__, name)
